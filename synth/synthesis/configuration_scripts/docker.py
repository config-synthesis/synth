"""Synth Dockerfile parsing."""


# Imports.
import math
import re
from collections.abc import Generator, Sequence
from contextlib import contextmanager, nullcontext
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Optional, Union

from docker import client
from docker.client import DockerClient
from docker.errors import ContainerError
from docker.models.images import Image
from docker.types import Mount
from more_itertools import chunked_even

from synth.logging import logger
from synth.synthesis.classes import (
    ConfigurationSystem,
    ConfigurationTask,
)
from synth.synthesis.configuration_scripts.classes import ParseResult
from synth.synthesis.configuration_scripts.shell import (
    parse_shell_script,
)
from synth.synthesis.exceptions import ParseException
from synth.util.shell import join


# Constants.
MAX_LAYERS = 127


@contextmanager
def _dockerfile(dockerfile: str) -> Generator[Path, None, None]:
    """Create a temporary Dockerfile for Dockerfile contents.

    Parameters
    ----------
    dockerfile : str
        Contents of a Dockerfile file.

    Yields
    ------
    Path
        Temporary Dockerfile path.
    """
    with NamedTemporaryFile() as file:
        file_path = Path(file.name)
        file_path.write_text(dockerfile)
        yield file_path


def normalize_run_command(run_command: str) -> str:
    """Normalize a Docker RUN command.

    Parameters
    ----------
    run_command : str
        Run command string.

    Returns
    -------
    str
        Normalized run command in shell format.
    """
    # Remove newlines
    run_command = run_command.replace('\\\n', '')

    # Check to see if run command is in exec form
    match = re.match(r'\["([^"]*)"(?: *, *"([^"]*)")*]', run_command)

    # If it matches, find all quoted commands
    if match:
        run_command = ' '.join(re.findall(r'"([^"]*)"', run_command))

    return run_command


def parse_dockerfile(dockerfile: Union[Path, str],
                     context: Optional[Path] = None) -> ParseResult:
    """Parse a Dockerfile for configuration tasks in run commands.

    Mounts will be parsed if a context directory is provided. Mounts are
    intended to replicate Docker's `COPY` and `ADD` instructions to provide
    files at the expected locations during synthesis. They are parsed according
    to the following rules:

    1. All source paths are interpreted as relative to the build context.
    2. If the destination is a relative path, it is interpreted as relative to
       the current WORKDIR.
    2. If the source is a directory, the destination is a directory and the
       source's contents are mounted into it.
    3. If the source is a file:
       a. If the destination ends with /, then it is treated as a directory and
          the file is mounted into it.
       b.
            i. If the destination does not exist or is a file, then the source
               is mounted as a file.
           ii. If the destination is a directory, the file is mounted into it.

    Parameters
    ----------
    dockerfile : Union[Path, str]
        The Dockerfile to parse. This should either be the Dockerfile text or a
        path to a readable file.
    context : Optional[Path]
        Path to an optional context directory. If specified, the Dockerfile
        will be built and the resulting image and context directory will be
        used to provide additional metadata in the parse result.

    Raises
    ------
    ValueError
        Raised if the Dockerfile does not exist.
    ParseError
        Raised if unable to parse the Dockerfile.

    Returns
    -------
    ParseResult
        Configuration tasks and metadata parsed from the Dockerfile.
    """
    if isinstance(dockerfile, Path):
        if not dockerfile.exists():
            raise ValueError('Dockerfile does not exist.')

        dockerfile_ctx = nullcontext(dockerfile)
    else:
        dockerfile_ctx = _dockerfile(dockerfile)

    with dockerfile_ctx as dockerfile_path:

        # Match all commands, including multiline commands with an escaped
        # newline. Then parse each command for configuration tasks.
        base_image = None
        workdir = Path('/')
        mounts = []
        script_parts = []
        lines = re.findall(
            r'^(\w+)\s+((?:(?:.*?\\\n)|(?:\s*#.*?\n))*.*?(?:\n|$))',
            dockerfile_path.read_text(),
            re.MULTILINE
        )
        for instruction_name, instruction_value in lines:
            if instruction_name == 'FROM':
                base_image = instruction_value.strip().split(' ')[0]
            if instruction_name == 'RUN':
                script_parts.append(normalize_run_command(instruction_value))
            elif instruction_name == 'ENV':
                script_parts.append(f'export {instruction_value}')
            elif instruction_name == 'ARG':
                script_parts.append(instruction_value)
            elif instruction_name == 'COPY' or instruction_name == 'ADD':
                *sources, target = instruction_value.strip().split(' ')
                mounts.extend(
                    Mount(
                        source=source,
                        target=str(workdir / target),
                        type='bind',
                        read_only=True,
                    )
                    for source in sources
                )
            elif instruction_name == 'WORKDIR':
                workdir = workdir / instruction_value.strip()

        if context is not None:
            logger.verbose(
                'A context directory was provided with the Dockerfile. '
                'Attempting to resolve mounts from COPY and ADD instructions. '
            )

            # Determine if we will need the image to infer correct mounds.
            needs_image = any(
                (
                    (context / mount['Source']).is_file()
                    and not mount['Target'].endswith('/')
                )
                for mount in mounts
            )
            if needs_image:
                logger.verbose(
                    '(Re)building the synth-parser image using the Dockerfile '
                    'and build context.'
                )
                docker_client: DockerClient = client.from_env()
                image: Image
                tag = 'synth-parser'
                image, _ = docker_client.images.build(
                    path=str(context),
                    dockerfile=str(dockerfile_path),
                    tag=tag,
                    network_mode='synth_default',
                )

            new_mounts = []
            for mount in mounts:
                src_path = context / mount['Source']
                target = mount['Target']
                target_path = Path(target)
                if not src_path.exists():
                    raise ParseException(
                        f'Mount source path `{src_path}` does not exist.'
                    )
                elif src_path.is_dir():
                    new_mounts.extend(
                        Mount(
                            source=str(child),
                            target=str(target_path / child.name),
                            type='bind',
                            read_only=True
                        )
                        for child in src_path.glob('*')
                    )
                elif src_path.is_file():
                    if target.endswith('/'):
                        new_mounts.append(Mount(
                            source=str(src_path),
                            target=str(target_path / src_path.name),
                            type='bind',
                            read_only=True,
                        ))
                    else:
                        try:
                            docker_client.containers.run(
                                image=image.id,
                                entrypoint=['test', '-d', target],
                                remove=True,
                            )
                        except ContainerError:
                            new_mounts.append(Mount(
                                source=str(src_path),
                                target=target,
                                type='bind',
                                read_only=True,
                            ))
                        else:
                            new_mounts.append(Mount(
                                source=str(src_path),
                                target=str(target_path / src_path.name),
                                type='bind',
                                read_only=True,
                            ))
                else:
                    raise ParseException(
                        f'Unrecognized source type for `{src_path}`.'
                    )

            mounts = new_mounts

            if needs_image:
                docker_client.images.remove(tag, force=True)

    # Parse tasks.
    result = parse_shell_script('\n'.join(script_parts))
    result.base_image = base_image
    result.mounts.extend(mounts)
    return result


def write_dockerfile(tasks: Sequence[ConfigurationTask]) -> str:
    """Write a Dockerfile from a sequence of configuration tasks.

    Parameters
    ----------
    tasks : Sequence[ConfigurationTask]
        Configuration tasks for the Dockerfile.

    Returns
    -------
    str
        Dockerfile contents.
    """
    if any(task.system != ConfigurationSystem.SHELL for task in tasks):
        raise ValueError(
            'Cannot write Dockerfile. All tasks must be shell tasks',
        )

    # Docker images can have a maximum of 127 layers. If there are more tasks
    # than that, they will be grouped evenly so that there are a maximum of
    # 127 RUN commands.
    chunk_size = math.ceil(len(tasks) / MAX_LAYERS)
    chunks = chunked_even(tasks, chunk_size)
    chunk_strings = [
        ' \\\n    && '.join([
            join([task.executable, *task.arguments])
            for task in chunk
        ])
        for chunk in chunks
    ]

    parts = [
        'FROM debian:11',
        '',
        *(
            f'RUN {chunk_string}'
            for chunk_string in chunk_strings
        )
    ]
    return '\n'.join(parts)
