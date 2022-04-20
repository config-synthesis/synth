"""Docker utilities for Synthesis."""


# Imports.
from __future__ import annotations

import json
import math
from abc import abstractmethod
from collections.abc import Generator, Sequence
from contextlib import (
    AbstractContextManager,
    contextmanager,
    ExitStack,
    nullcontext,
)
from csv import DictReader
from dataclasses import dataclass
from difflib import SequenceMatcher
from itertools import chain
from json import JSONDecodeError
from os import readlink
from os.path import basename, normpath, splitext
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep
from types import TracebackType
from typing import Any, Optional, Union

import sh
import yaml
from docker import client
from docker.client import DockerClient
from docker.models.containers import Container
from docker.models.images import Image
from docker.types import Mount

from synth.logging import logger
from synth.synthesis.classes import (
    AnsibleTaskError,
    ConfigurationChange,
    ConfigurationSystem,
    ConfigurationTask,
    ConfigurationTaskArgument,
    ConfigurationTaskError,
    DirectoryAdd,
    DirectoryDelete,
    EnvSet,
    EnvUnset,
    FileAdd,
    FileChange,
    FileContentChange,
    FileContentChangeType,
    FileDelete,
    frozendict,
    ServiceStart,
    ServiceStop,
    ShellTaskError,
    SymbolicLink,
    WorkingDirectorySet,
)
from synth.synthesis.configuration_scripts import (
    get_parser,
    parse_shell_script,
)
from synth.synthesis.configuration_scripts.ansible import write_playbook
from synth.synthesis.configuration_scripts.classes import ParseResult
from synth.synthesis.exceptions import DockerException
from synth.synthesis.knowledge_base import (
    insert_task_executions,
    ResolvingTaskTuple,
)
from synth.util.shell import join
from synth.util.timeout import Timeout


# Paths
_DOCKER_RUNNERS = Path(__file__).parent / 'docker_runners'


# Other metadata.
_IMAGE_DIFF_PREFIX_EXCLUDES = frozenset({
    '/tmp/systemd-private-',      # noqa: S108
    '/validation',
    '/var/log',
    '/var/tmp/systemd-private-',  # noqa: S108
})
_IMAGE_DIFF_EXT_EXCLUDES = frozenset({
    '.pyc',
})
_IMAGE_DIFF_NAME_EXCLUDES = frozenset({
    '__pycache__',
})
_BINARY_FILE_PATH_PREFIXES = frozenset({
    '/bin',
    '/sbin',
    '/usr/bin',
    '/usr/sbin',
    '/usr/local/bin',
    '/usr/local/sbin',
})
_SYSTEMD_ACTIVE_STATES = {'active', 'activating', 'reloading'}
_SYSTEMD_INACTIVE_STATES = {'inactive', 'deactivating', 'failed'}
_ENTRYPOINT = ('/scripts/run_command.sh',)


# Constants.
ANALYSIS_TIMEOUT = 10800  # 3 hours.


# Docker build info.
@dataclass(frozen=True)
class DockerBuildContext:
    """Dataclass for Docker build information."""

    tag: str
    dockerfile: Path
    context_dir: Path


_ANSIBLE_RUNNER_CONTEXT = DockerBuildContext(
    tag='synth/ansible-task-runner:latest',
    dockerfile=_DOCKER_RUNNERS / 'AnsibleTaskRunner.Dockerfile',
    context_dir=_DOCKER_RUNNERS,
)
_SHELL_RUNNER_CONTEXT = DockerBuildContext(
    tag='synth/shell-task-runner:latest',
    dockerfile=_DOCKER_RUNNERS / 'ShellTaskRunner.Dockerfile',
    context_dir=_DOCKER_RUNNERS,
)


def get_runner(system: Union[str, ConfigurationSystem], **kwargs
               ) -> ConfigurationTaskRunner:
    """Get a configuration task runner for the system.

    Parameters
    ----------
    system : Union[str, ConfigurationSystem]
        A configuration system.

    Raises
    ------
    ValueError
        Raised if ``system`` is not a known configuration system.

    Returns
    -------
    ConfigurationTaskRunner
        The configuration task runner for the specified configuration system.
    """
    # Get the enum value of system.
    system = ConfigurationSystem(system)

    if system in (ConfigurationSystem.SHELL, ConfigurationSystem.DOCKER):
        return ShellTaskRunner(**kwargs)
    elif system == ConfigurationSystem.ANSIBLE:
        return AnsibleTaskRunner(**kwargs)
    else:
        raise ValueError('Unrecognized system.')


@dataclass(frozen=True)
class RunResult:
    """The result of running a configuration task.

    Attributes
    ----------
    exit_code : int
        The task exit code.
    stdout : str
        The task stdout.
    stderr : str
        The task stderr.
    """

    exit_code: int
    stdout: str
    stderr: str


def _prepare_container(container: Container, install_python3: bool = False):
    """Wait for a container to be ready and then finish preparing it.

    This method runs apt-get update until the command succeeds, finishes
    preparation, then cleans up and exits.

    Parameters
    ----------
    container : Container
        Container to wait for.
    install_python3 : bool
        If true, Python3 will be installed when the container is ready. Useful
        for systems like Ansible that require Python to be installed.

    Raises
    ------
    TimeoutError
        Raised if the container is not ready within the timeout.
    """
    # Wait for the container to be ready by running apt-get update until
    # it succeeds.
    logger.debug('Preparing container.')
    try:
        has_apt_lists_exit_code, _ = container.exec_run(
            cmd=[
                'sh',
                '-c',
                'find /var/lib/apt/lists/ -mindepth 1 -maxdepth 1 '
                '| grep -q .',
            ],
            demux=True,
        )
        has_apt_lists = not has_apt_lists_exit_code
        if has_apt_lists:
            logger.spam('Container has pre-existing /var/lib/apt/lists.')

        logger.debug('Entering startup loop.')
        with Timeout(seconds=30):
            while True:
                exit_code, _ = container.exec_run(
                    cmd=['apt-get', 'update'],
                    demux=True,
                )
                if not exit_code:
                    break
                else:
                    logger.spam('Waiting for container to be up...')
                    sleep(1)

        logger.verbose('Docker container is up.')

        if install_python3:
            logger.spam('Installing Python3.')
            exit_code, _ = container.exec_run(
                cmd=[
                    'apt-get', 'install', '-y', '--no-install-recommends',
                    'python3'
                ],
                demux=True,
            )
            if exit_code:
                raise DockerException('Unable to install Python3.')

        if not has_apt_lists:
            logger.spam('Removing /var/lib/apt/lists.')
            exit_code, (stdout, stderr) = container.exec_run(
                cmd=['sh', '-c', 'rm -r /var/lib/apt/lists/*'],
                demux=True,
            )
            if exit_code:
                raise DockerException(
                    f'Unable to cleanup after container ready: '
                    f'{stderr}'
                )

        logger.verbose('Docker container is ready.')
    except TimeoutError:
        logger.exception('Docker container was not up by timeout.')
        raise


class DockerRunContext(AbstractContextManager):
    """Docker run context manager.

    Starts a Docker container when entering the context, then stops and removes
    it when exiting the context.
    """

    # Default keyword arguments for Docker run.
    _run_defaults = (
        ('image', 'alpine'),
        ('detach', True),
        ('privileged', True),
        ('stdin_open', True),
        ('stdout', True),
        ('stderr', True),
        ('command', '/bin/systemd'),
        ('network', 'synth_default'),
        ('environment', {
            'DEBIAN_FRONTEND': 'noninteractive',
        }),
    )

    def __init__(self,
                 install_python3: bool = False,
                 **run_kwargs: dict[str, Any]):
        """Create a new Docker run context.

        Parameters
        ----------
        run_kwargs : dict[str, Any]
            Keyword arguments for Docker ``client.containers.run``.
        """
        self.client: DockerClient = client.from_env()
        self.container: Optional[Container] = None
        self.install_python3 = install_python3
        self.run_kwargs = run_kwargs

        for key, value in self._run_defaults:
            if key not in self.run_kwargs:
                self.run_kwargs[key] = value

    def __enter__(self) -> DockerRunContext:
        """Start the Docker container."""
        logger.verbose('Entering Docker context...')
        self.container = self.client.containers.run(**self.run_kwargs)
        _prepare_container(
            container=self.container,
            install_python3=self.install_python3,
        )
        logger.verbose('Docker container created.')
        return self

    def __exit__(self,
                 exc_type: Optional[type],
                 exc_val: Optional[BaseException],
                 exc_tb: Optional[TracebackType]):
        """Stop the Docker container and remove it."""
        logger.verbose('Exiting Docker context...')
        self.container.stop()
        self.container.remove()
        self.client.close()
        logger.verbose('Docker container removed.')


class ConfigurationTaskRunner(DockerRunContext):
    """A runner for configuration tasks."""

    build_context: DockerBuildContext
    _built_image: bool = False

    def __new__(cls, *args, **kwargs) -> ConfigurationTaskRunner:
        """Create a new task runner.

        This implementation rebuilds the runner image on creation.
        """
        obj = super().__new__(cls)
        cls.build_runner_image()
        return obj

    def __init__(self,
                 is_runner_image: bool = True,
                 task_entrypoint: Sequence[str] = _ENTRYPOINT,
                 **kwargs):
        """Initialize a new task runner.

        This implementation sets Docker context run defaults.

        Parameters
        ----------
        is_runner_image : bool
            Whether the runner Docker image is, or is based on, the default
            Synth runner image for the system. If false, the runner should
            enter a compatibility mode and mount the appropriate run scripts
            into the run container.
        task_entrypoint : Sequence[str]
            The entrypoint that should be used for running tasks. By default
            this is the Synth run_command script that records configuration
            metadata.
        """
        if hasattr(self, 'build_context') and 'image' not in kwargs:
            kwargs['image'] = self.build_context.tag
        super().__init__(**kwargs)

        self.is_runner_image = is_runner_image
        self.task_entrypoint = task_entrypoint

    @classmethod
    def build_runner_image(cls):
        """Build the runner image."""
        if not hasattr(cls, 'build_context'):
            return

        if cls._built_image:
            return

        logger.verbose('(Re)building runner image.')
        docker_client: DockerClient = client.from_env()
        docker_client.images.build(
            path=str(cls.build_context.context_dir),
            dockerfile=str(cls.build_context.dockerfile),
            tag=cls.build_context.tag,
            network_mode='synth_default',
        )
        cls._built_image = True

    @abstractmethod
    def run_task(self,
                 task: ConfigurationTask,
                 arguments: frozenset[ConfigurationTaskArgument] = frozenset(),
                 timeout: Optional[int] = None) -> RunResult:
        """Run a configuration task.

        Parameters
        ----------
        task : ConfigurationTask
            The task to run.
        arguments : frozenset[ConfigurationTaskArguments]
            Configuration task arguments that may be present in ``task`` and
            should be used to construct a configuration task error if the task
            fails.
        timeout : Optional[int]
            Timeout in seconds. If provided the task will be stopped if
            execution takes longer than the timeout.

        Raises
        ------
        ConfigurationTaskError
            Raised when the task fails to run successfully.
        TimeoutError
            Raised if execution times out.

        Returns
        -------
        RunResult
            The result of running the task.
        """


class AnsibleTaskRunner(ConfigurationTaskRunner):
    """A Docker runner for Ansible tasks."""

    build_context = _ANSIBLE_RUNNER_CONTEXT

    def __init__(self, *args, **kwargs):
        """Initialize an Ansible task runner."""
        super().__init__(*args, install_python3=True, **kwargs)
        self.run_container: Optional[Container] = None

        if not self.is_runner_image:
            self.run_kwargs |= {
                'entrypoint': ['sh'],
                'command': None,
            }
            self.run_container_kwargs = self.run_kwargs | {
                'image': self.build_context.tag,
                'mounts': [
                    *self.run_kwargs.get('mounts', []),
                    Mount(
                        type='bind',
                        source='/var/run/docker.sock',
                        target='/var/run/docker.sock',
                    ),
                    Mount(
                        type='bind',
                        source=str(
                            Path(__file__).parent
                            / 'docker_runners/run_command.sh'
                        ),
                        target='/scripts/run_command.sh',
                        read_only=True,
                    ),
                    Mount(
                        type='bind',
                        source=str(
                            Path(__file__).parent
                            / 'docker_runners/cleanup.sh'
                        ),
                        target='/scripts/cleanup.sh',
                        read_only=True,
                    ),
                ],
                'environment': self.run_kwargs.get('environment', {}) | {
                    'SYNTH_NO_METADATA': '',
                },
            }
        else:
            self.run_container_kwargs = self.run_kwargs

    def __enter__(self) -> AnsibleTaskRunner:
        """Enter the run context.

        This context also creates an associated Ansible run container if not
        running in the default runner image.

        Returns
        -------
        AnsibleTaskRunner
            ``self``.
        """
        super().__enter__()

        if self.is_runner_image:
            self.run_container = self.container
        else:
            logger.verbose('Starting Ansible run container.')
            self.run_container = self.client.containers.run(
                **self.run_container_kwargs
            )
            _prepare_container(self.run_container, install_python3=True)
            logger.verbose('Ansible run container created.')

        return self

    def __exit__(self,
                 exc_type: Optional[type],
                 exc_val: Optional[BaseException],
                 exc_tb: Optional[TracebackType]):
        """Exit the run context."""
        if self.container != self.run_container:
            logger.verbose('Removing Ansible run container.')
            self.run_container.stop()
            self.run_container.remove()
            logger.verbose('Ansible run container removed.')
        super().__exit__(exc_type, exc_val, exc_tb)

    def run_task(self,
                 task: ConfigurationTask,
                 arguments: frozenset[ConfigurationTaskArgument] = frozenset(),
                 timeout: Optional[int] = None) -> RunResult:
        """Run an Ansible task.

        Parameters
        ----------
        task : ConfigurationTask
            The task to run.
        arguments : frozenset[ConfigurationTaskArguments]
            Configuration task arguments that may be present in ``task`` and
            should be used to construct a configuration task error if the task
            fails.
        timeout : Optional[int]
            Timeout in seconds. If provided the task will be stopped if
            execution takes longer than the timeout.

        Raises
        ------
        AnsibleTaskError
            Raised when the task fails to run successfully.
        TimeoutError
            Raised if execution times out.

        Returns
        -------
        RunResult
            The result of running the task.
        """
        logger.verbose(f'Running task: `{task}`')

        if self.run_container == self.container:
            cmd = ['ansible-playbook playbook.yml']
            host = 'localhost'
        else:
            cmd = [
                f"ansible-playbook -i '{self.container.name},' "
                f"-c docker playbook.yml"
            ]
            host = self.container.name

        if self.task_entrypoint:
            cmd = [*self.task_entrypoint, *cmd]

        playbook = write_playbook([task], hosts=host, become=False)

        if timeout is not None:
            timeout_ctx = Timeout(seconds=timeout)
        else:
            timeout_ctx = nullcontext()

        with timeout_ctx:
            exit_code, (stdout, stderr) = self.run_container.exec_run(
                cmd=cmd,
                demux=True,
                environment={'PLAYBOOK': playbook},
            )

        if stdout is not None:
            stdout = str(stdout, 'utf-8')
        else:
            stdout = ''

        if stderr is not None:
            stderr = str(stderr, 'utf-8')
        else:
            stderr = ''

        if exit_code:
            try:
                output = json.loads(stdout)
                task_output = output['plays'][0]['tasks'][1]['hosts'][host]
                json_output = json.dumps(task_output)
            except (JSONDecodeError, IndexError):
                logger.exception(
                    f'Exception encountered while processing Ansible output:\n'
                    f'stdout:\n{stdout}\n\n'
                    f'stderr:\n{stderr}'
                )
                raise

            logger.verbose(
                f'\n'
                f'Task failed with error:\n'
                f'    msg: {task_output["msg"]}\n'
            )
            raise AnsibleTaskError.from_json(
                json_output=json_output,
                arguments=arguments,
            )

        return RunResult(
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr
        )


class ShellTaskRunner(ConfigurationTaskRunner):
    """A Docker runner for shell tasks."""

    build_context = _SHELL_RUNNER_CONTEXT

    def __init__(self, *args, **kwargs):
        """Initialize a shell task runner."""
        super().__init__(*args, **kwargs)

        if not self.is_runner_image:
            self.run_kwargs |= {
                'entrypoint': ['sh'],
                'command': None,
                'mounts': [
                    *self.run_kwargs.get('mounts', []),
                    Mount(
                        type='bind',
                        source=str(
                            Path(__file__).parent
                            / 'docker_runners/run_command.sh'
                        ),
                        target='/scripts/run_command.sh',
                        read_only=True,
                    ),
                    Mount(
                        type='bind',
                        source=str(
                            Path(__file__).parent
                            / 'docker_runners/cleanup.sh'
                        ),
                        target='/scripts/cleanup.sh',
                        read_only=True,
                    ),
                ],
                'environment': self.run_kwargs.get('environment', {}) | {
                    'SYNTH_NO_METADATA': '',
                },
            }

    def run_task(self,
                 task: ConfigurationTask,
                 arguments: frozenset[ConfigurationTaskArgument] = frozenset(),
                 timeout: Optional[int] = None) -> RunResult:
        """Run a shell task.

        Parameters
        ----------
        task : ConfigurationTask
            The task to run.
        arguments : frozenset[ConfigurationTaskArguments]
            Configuration task arguments that may be present in ``task`` and
            should be used to construct a configuration task error if the task
            fails.
        timeout : Optional[int]
            Timeout in seconds. If provided the task will be stopped if
            execution takes longer than the timeout.

        Raises
        ------
        ShellTaskError
            Raised when the task fails to run successfully.
        TimeoutError
            Raised if execution times out.

        Returns
        -------
        RunResult
            The result of running the task.
        """
        # This will not preserve state like shell options, CWD, or
        # environment variables between sessions. If these are needed,
        # one option would be to use a wrapping exec script to save/restore
        # state in coordination with the `exec_run` parameters `cwd` and
        # `environment`.
        logger.verbose(f'Running task: `{task}`')

        cmd = [join([task.executable, *task.arguments])]
        if self.task_entrypoint:
            cmd = [*self.task_entrypoint, *cmd]

        if timeout is not None:
            timeout_ctx = Timeout(seconds=timeout)
        else:
            timeout_ctx = nullcontext()

        with timeout_ctx:
            exit_code, (stdout, stderr) = self.container.exec_run(
                cmd=cmd,
                demux=True,
            )

        if stdout is not None:
            stdout = str(stdout, 'utf-8')
        else:
            stdout = ''

        if stderr is not None:
            stderr = str(stderr, 'utf-8')
        else:
            stderr = ''

        # If there is a non-zero exit code, raise a shell task error.
        if exit_code:
            logger.verbose(
                f'\n'
                f'Task failed with error:\n'
                f'    exit_code: {exit_code}\n'
                f'    stdout:    {stdout}\n'
                f'    stderr:    {stderr}'
            )
            raise ShellTaskError.from_primitives(
                exit_code=exit_code,
                stdout=stdout,
                stderr=stderr,
                arguments=arguments,
            )

        # Return the successful result.
        return RunResult(
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
        )


@contextmanager
def cleanup_images(docker_client: DockerClient,
                   labels: dict[str, str]) -> Generator[None, None, None]:
    """Cleanup Docker images.

    Parameters
    ----------
    docker_client : DockerClient
        An active Docker client.
    labels : dict[str, str]
        Docker image labels (key, value). At least one label must be provided.
        Images matching all labels will be removed on exiting the cleanup
        context.

    Raises
    ------
    ValueError
        Raised if no labels are provided.
    """
    if not labels:
        raise ValueError('At least one label must be provided.')

    try:
        yield
    finally:
        image_ids: list[str] = docker_client.api.images(
            quiet=True,
            filters={
                'label': [f'{key}={value}' for key, value in labels.items()],
            },
        )
        if image_ids:
            logger.info('Cleaning up Docker images.')
            with logger.indent():
                for image_id in image_ids:
                    image = docker_client.images.get(image_id)
                    if image.tags:
                        tag_str = f': `{image.tags}`'
                    else:
                        tag_str = ''
                    logger.verbose(f'Cleaning up image `{image.id}`{tag_str}.')
                    docker_client.images.remove(image.id)


def _get_cached_path(cache: Path, path: Union[str, Path]) -> Path:
    """Get an absolute path to a location within a cache.

    This method converts an absolute file path in a filesystem to an absolute
    filepath for that filesystem stored at a cache path. It also operates like
    readlink and resolves the value of symbolic links.

    Parameters
    ----------
    cache : Path
        Absolute path to the filesystem cache root.
    path : Union[str, Path]
        Absolute path to a file within the filesystem.

    Returns
    -------
    Path
        An absolute path to the final location of the file within the cache.
    """
    path = cache / Path(path).relative_to('/')
    explored_paths = set()

    while path.is_symlink():
        # If this path has been seen before, then there is a cycle of symlinks.
        # Just return the current path.
        if path in explored_paths:
            logger.warning(f'Symlink cycle encountered with path `{path}`.')
            return path

        explored_paths.add(path)

        link = path.readlink()

        if link.is_absolute():
            path = cache / link.relative_to('/')
        else:
            # Do not use Path.resolve, since it will attempt to follow symlinks
            # automatically.
            path = Path(normpath(path.parent.joinpath(link)))

    return path


def _relative_to_runner_dir(file: Union[Path, str]) -> str:
    """Make a file path relative to the runner working directory.

    Parameters
    ----------
    file : Union[Path, str]
        File path.

    Returns
    -------
    str
        Either a path relative to the runner working directory or the original
        file path.
    """
    try:
        return str(Path(file).relative_to('/root/runner'))
    except ValueError:
        return str(file)


def diff_files(image1_cache: Path,
               image2_cache: Path,
               file: Path) -> FileChange:
    """Compute a file change from the diff of a file between two images.

    The file must exist in at least one of the two cache paths. If it only
    exists in one, the content of the other file is treated as the empty
    string.

    Parameters
    ----------
    image1_cache : Path
        Local filesystem cache for the first image.
    image2_cache : Path
        Local filesystem cache for the second image.
    file : Path
        Path of a file found in both images.

    Raises
    ------
    ValueError
        Raised if one of the cache directories does not exist, if the file
        does not exist in either directory, or if the file path is not a file
        in either cache.

    Returns
    -------
    FileChange
        A file change from the first to second image.
    """
    if not image1_cache.is_dir() or not image2_cache.is_dir():
        raise ValueError('Cache paths must exist and be directories.')

    # Get the image file paths and read lines.
    image1_file_path = _get_cached_path(image1_cache, file)
    image2_file_path = _get_cached_path(image2_cache, file)

    if ((not image1_file_path.exists() and not image2_file_path.exists())
            or (image1_file_path.exists() and not image1_file_path.is_file())
            or (image2_file_path.exists() and not image2_file_path.is_file())):
        raise ValueError(
            'File must exist in at least one cache and be a file.'
        )

    try:
        with open(image1_file_path, 'r') as fd:
            image1_file_lines = fd.readlines()
    except FileNotFoundError:
        image1_file_lines = []

    try:
        with open(image2_file_path, 'r') as fd:
            image2_file_lines = fd.readlines()
    except FileNotFoundError:
        image2_file_lines = []

    # Compute changes.
    matcher = SequenceMatcher(None, image1_file_lines, image2_file_lines)
    changes = set()
    for code, i1, i2, j1, j2 in matcher.get_opcodes():
        if code == 'equal':
            continue

        if code == 'replace' or code == 'delete':
            changes.add(FileContentChange.from_primitives(
                change_type=FileContentChangeType.DELETION,
                content=''.join(image1_file_lines[i1:i2]),
            ))
        if code == 'replace' or code == 'insert':
            changes.add(FileContentChange.from_primitives(
                change_type=FileContentChangeType.ADDITION,
                content=''.join(image2_file_lines[j1:j2])
            ))

    return FileChange.from_primitives(
        path=_relative_to_runner_dir(file),
        changes=frozenset(changes),
    )


def _process_add(image1_cache: Path,
                 image2_cache: Path,
                 file: str) -> frozenset[ConfigurationChange]:
    """Process all adds from an image diff.

    Parameters
    ----------
    image1_cache : Path
        First image cache directory.
    image2_cache : Path
        Second image cache directory.
    file : str
        File path within each image.

    Returns
    -------
    frozenset[ConfigurationChange]
        All configuration changes needed to represent the add.
    """
    changes = []
    file_path = Path(file)

    image2_original_file_path = image2_cache / file_path.relative_to('/')
    image2_file_path = _get_cached_path(image2_cache, file)

    if image2_original_file_path.is_symlink():
        link = Path(readlink(image2_original_file_path))

        # If the link is not absolute, turn it into an absolute path by joining
        # with the file path parent directory and normalizing.
        #
        # The base file path must be used instead of the cached file path so
        # that edge cases that go back too many levels don't go past root.
        #
        # Normpath is used because Path.resolve will attempt to follow symlinks
        # automatically.
        if not link.is_absolute():
            link = Path(normpath(file_path.parent.joinpath(link)))

        changes.append(SymbolicLink.from_primitives(
            path=_relative_to_runner_dir(file),
            link=_relative_to_runner_dir(link),
        ))
    elif image2_file_path.is_dir():
        changes.append(DirectoryAdd.from_primitives(
            path=_relative_to_runner_dir(file)),
        )
    elif image2_file_path.is_file():
        changes.append(FileAdd.from_primitives(
            path=_relative_to_runner_dir(file)),
        )

        # If the file is not a binary file, get a FileContentChange for its
        # text changes.
        if image2_file_path.stat().st_size > 0:
            try:
                changes.append(diff_files(
                    image1_cache,
                    image2_cache,
                    file_path,
                ))
            except UnicodeDecodeError:
                pass
    else:
        if image2_file_path.exists():
            raise ValueError('Add must represent a file or directory.')
        else:
            logger.warning(f'Add path `{image2_file_path}` does not exist.')

    return frozenset(changes)


def _process_del(image1_cache: Path,
                 image2_cache: Path,
                 file: str) -> frozenset[ConfigurationChange]:
    """Process all dels from an image diff.

    Parameters
    ----------
    image1_cache : Path
        First image cache directory.
    image2_cache : Path
        Second image cache directory.
    file : str
        File path within each image.

    Returns
    -------
    frozenset[ConfigurationChange]
        All configuration changes needed to represent the del.
    """
    file_path = Path(file)
    image1_file_path = _get_cached_path(image1_cache, file_path)

    if image1_file_path.is_dir():
        return frozenset({DirectoryDelete.from_primitives(
            path=_relative_to_runner_dir(file),
        )})
    elif image1_file_path.is_file():
        return frozenset({FileDelete.from_primitives(
            path=_relative_to_runner_dir(file),
        )})
    else:
        if image1_file_path.exists():
            raise ValueError('Del must represent a file or directory.')
        else:
            logger.warning(f'Del path `{image1_file_path}` does not exist.')
            return frozenset()


def _process_mod(image1_cache: Path,
                 image2_cache: Path,
                 file: str) -> frozenset[ConfigurationChange]:
    """Process all mods from an image diff.

    Parameters
    ----------
    image1_cache : Path
        First image cache directory.
    image2_cache : Path
        Second image cache directory.
    file : str
        File path within each image.

    Returns
    -------
    frozenset[ConfigurationChange]
        All configuration changes needed to represent the mod.
    """
    file_path = Path(file)
    image1_file_path = _get_cached_path(image1_cache, file_path)

    if image1_file_path.is_dir():
        return frozenset({})
    elif image1_file_path.is_file():
        try:
            return frozenset({
                diff_files(image1_cache, image2_cache, file_path)
            })
        except UnicodeDecodeError:
            file = _relative_to_runner_dir(file)
            return frozenset({
                FileDelete.from_primitives(path=file),
                FileAdd.from_primitives(path=file),
            })
    else:
        if image1_file_path.exists():
            raise ValueError('Mod must represent a file or directory.')
        else:
            logger.warning(f'Mod path `{image1_file_path}` does not exist.')
            return frozenset()


def _process_cwd(image2_pre: Path,
                 image2_post: Path) -> frozenset[ConfigurationChange]:
    """Process CWD metadata.

    Parameters
    ----------
    image2_pre : Path
        Resulting image pre-run metadata directory.
    image2_post : Path
        Resulting image post-run metadata directory.

    Returns
    -------
    frozenset[ConfigurationChange]
        All configuration changes needed to represent a cwd change.
    """
    cwd_pre = (image2_pre / 'cwd').read_text().strip()
    cwd_post = (image2_post / 'cwd').read_text().strip()

    if cwd_pre != cwd_post:
        return frozenset({
            WorkingDirectorySet.from_primitives(path=cwd_post),
        })

    return frozenset()


def _process_env(image2_pre: Path,
                 image2_post: Path) -> frozenset[ConfigurationChange]:
    """Process env metadata.

    Parameters
    ----------
    image2_pre : Path
        Resulting image pre-run metadata directory.
    image2_post : Path
        Resulting image post-run metadata directory.

    Returns
    -------
    frozenset[ConfigurationChange]
        All configuration changes needed to represent an env changes.
    """
    with open(image2_pre / 'env', 'r') as fd:
        env_pre = {
            key: value
            for key, value in [
                line.strip().split('=')
                for line in fd.readlines()
            ]
        }

    with open(image2_post / 'env', 'r') as fd:
        env_post = {
            key: value
            for key, value in [
                line.strip().split('=')
                for line in fd.readlines()
            ]
        }

    return frozenset(chain(
        (
            EnvUnset.from_primitives(key=key)
            for key in env_pre.keys()
            if key not in env_post
        ),
        (
            EnvSet.from_primitives(
                key=key,
                value=env_post[key],
            )
            for key in env_post.keys()
            if key not in env_pre or env_pre[key] != env_post[key]
        )
    ))


def _process_services(image2_pre: Path,
                      image2_post: Path) -> frozenset[ConfigurationChange]:
    """Process service metadata.

    Parameters
    ----------
    image2_pre : Path
        Resulting image pre-run metadata directory.
    image2_post : Path
        Resulting image post-run metadata directory.

    Returns
    -------
    frozenset[ConfigurationChange]
        All configuration changes needed to represent service changes.
    """
    with open(image2_pre / 'services') as fd:
        services_pre = {
            service['UNIT']: service['ACTIVE']
            for service in DictReader(fd)
        }

    with open(image2_post / 'services') as fd:
        services_post = {
            service['UNIT']: service['ACTIVE']
            for service in DictReader(fd)
        }

    return frozenset(chain(
        (
            ServiceStart.from_primitives(name=service)
            for service, state in services_post.items()
            if (state in _SYSTEMD_ACTIVE_STATES
                and (service not in services_pre
                     or services_pre[service] in _SYSTEMD_INACTIVE_STATES))
        ),
        (
            ServiceStop.from_primitives(name=service)
            for service, state in services_pre.items()
            if (state in _SYSTEMD_ACTIVE_STATES
                and (service not in services_post
                     or services_post[service] in _SYSTEMD_INACTIVE_STATES))
        ),
    ))


def diff_images(image1: str,
                image2: str,
                cache_dir: Optional[Union[Path, str]] = None,
                mounts: Optional[list[Mount]] = None,
                ) -> frozenset[ConfigurationChange]:
    """Diff two images produced by a task runner.

    Parameters
    ----------
    image1 : str
        Base image identifier.
    image2 : str
        Changed image identifier.
    cache_dir : Optional[Union[Path, str]]
        Optional cache directory. If specified, the container diff filesystem
        cache will be placed there. Otherwise, a temporary directory will be
        used for cache.
    mounts : Optional[list[Mount]]
        Mounted file paths to exclude from the diff.

    Returns
    -------
    frozenset[ConfigurationChange]
        All configuration changes made going from ``image1`` to ``image2``.
    """
    # Get the correct directory context. If no directory was provided, this
    # should be a new temporary directory. Otherwise it will be a nullcontext
    # that yields the cache directory path.
    if cache_dir is not None:
        ctx = nullcontext(str(cache_dir))
    else:
        ctx = TemporaryDirectory()

    if mounts:
        mount_targets = {
            mount['Target']
            for mount in mounts
        }
    else:
        mount_targets = set()

    # Enter the directory context.
    with ctx as cache_name:
        # Run a file diff of the two images.
        logger.info(f'Diffing images: `{image1}`, `{image2}`.')
        running_cmd = sh.container_diff(
            'diff',
            f'--cache-dir={cache_name}',
            f'daemon://{image1}',
            f'daemon://{image2}',
            '--type=file',
            '--json',
        )
        container_diff = json.loads(running_cmd.stdout)[0]['Diff']

        # Normalize null values and filter commonly excluded files.
        logger.spam('Normalizing image diff items.')
        for name, items in container_diff.items():
            if items is None:
                container_diff[name] = []
            else:
                container_diff[name] = list(filter(
                    lambda item: (
                        not any(
                            item['Name'].startswith(prefix)
                            for prefix in _IMAGE_DIFF_PREFIX_EXCLUDES
                        )
                        and splitext(item['Name'])[1] not in (
                            _IMAGE_DIFF_EXT_EXCLUDES
                        )
                        and basename(item['Name']) not in (
                            _IMAGE_DIFF_NAME_EXCLUDES
                        )
                        and item['Name'] not in mount_targets
                    ),
                    items
                ))

        # Start the list of all changes seen so far.
        changes = []

        # Get cache paths.
        cache_path = Path(cache_name) / '.container-diff/cache'
        image1_cache_name = image1.replace('/', '').replace(':', '_')
        image1_cache = cache_path / f'daemon_{image1_cache_name}'
        image2_cache_name = image2.replace('/', '').replace(':', '_')
        image2_cache = cache_path / f'daemon_{image2_cache_name}'

        # Get all file adds.
        logger.spam('Diffing file adds.')
        for add in container_diff['Adds']:
            changes.append(_process_add(
                image1_cache,
                image2_cache,
                add['Name'],
            ))

        # Get all file deletions.
        logger.spam('Diffing file dels.')
        for deletion in container_diff['Dels']:
            changes.append(_process_del(
                image1_cache,
                image2_cache,
                deletion['Name'],
            ))

        # Get all file modifications.
        logger.spam('Diffing file mods.')
        for mod in container_diff['Mods']:
            changes.append(_process_mod(
                image1_cache,
                image2_cache,
                mod['Name'],
            ))

        # Get resulting image pre and post metadata directories.
        logger.spam('Diffing validation metadata.')
        image2_validation = image2_cache / 'validation'
        image2_pre = image2_validation / 'pre'
        image2_post = image2_validation / 'post'

        # Extract changes from metadata.
        if image2_validation.exists():
            changes.append(_process_cwd(image2_pre, image2_post))
            changes.append(_process_env(image2_pre, image2_post))
            changes.append(_process_services(image2_pre, image2_post))

    return frozenset(chain.from_iterable(changes))


@dataclass(frozen=True)
class AnalysisResult:
    """The result of analyzing a configuration script.

    Attributes
    ----------
    success : bool
        True iff analysis completed on the entire configuration script.
    tasks : frozenset[ResolvingTaskTuple]
        Configuration ask executions discovered during analysis.
    failed_at_task : Optional[ConfigurationTask]
        The task configuration analysis failed at. Defined iff success is
        False.
    configuration_task_error : Optional[ConfigurationTaskError]
        The error raised by running the failed task. Defined iff success is
        False.
    """

    success: bool
    tasks: frozenset[ResolvingTaskTuple]
    failed_at_task: Optional[ConfigurationTask] = None
    configuration_task_error: Optional[ConfigurationTaskError] = None


def analyze_configuration_script(
        context: Path,
        result: ParseResult,
        setup: Optional[ParseResult] = None) -> AnalysisResult:
    """Analyze a configuration script.

    A script is a sequence of one or more configuration tasks that can be run
    against a system. Analysis is performed by executing the configuration
    tasks and determining:

    1. What changes they make.
    2. What errors they experience, and what other configuration tasks resolve
       those errors.

    This is done by running through the configuration script one task at a
    time. At each step, the changes made from the previous step are recorded.
    All subsequent configuration tasks are then run against the resulting
    environment and checked for errors. Errors are marked as resolved when
    they disappear.

    Parameters
    ----------
    context : Path
        The context directory for the configuration script. This should be the
        path to the directory it expects to be run from. For Dockerfiles, this
        is the Docker build context.
    result : ParseResult
        The result of parsing a configuration script.
    setup : Optional[ParseResult]
        The result of parsing a setup configuration script.

    Returns
    -------
    AnalysisResult
        Data from running analysis.
    """
    tasks = result.tasks

    # Pre-process mounts for the runner paths.
    mounts = []
    for mount in result.mounts:
        new_mount = dict(mount)
        new_mount['Source'] = str(context / mount['Source'])
        mounts.append(new_mount)

    # Exit early if there are no tasks.
    if not tasks:
        logger.info('No configuration tasks to analyze.')
        return AnalysisResult(success=True, tasks=frozenset())

    # Log the script under analysis.
    width = math.ceil(math.log(len(tasks), 10))
    task_str = '\n'.join(
        f'    {idx:{width}.0f} {task}'
        for idx, task in enumerate(tasks)
    )
    logger.info(f'Analyzing configuration script:\n{task_str}')

    # Record the previous error encountered for all tasks and the range of
    # tasks for which that error was present.
    previous_errors: dict[ConfigurationTask, ConfigurationTaskError] = {}
    resolved_errors: dict[tuple[ConfigurationTask,
                                Optional[ConfigurationTaskError]],
                          Optional[tuple[int, int]]] = {}
    changes: dict[ConfigurationTask, frozenset[ConfigurationChange]] = {}

    def run_subsequent(system: ConfigurationSystem,
                       image: str,
                       task: ConfigurationTask,
                       idx: int):
        """Run a subsequent configuration task.

        This is a helper method to record error results.

        Parameters
        ----------
        system : ConfigurationSystem
            The configuration system to use.
        image : str
            The Docker runner image tag.
        task : ConfigurationTask
            The configuration task to run.
        idx : int
            The current index.
        """
        ctx = get_runner(system, image=image, mounts=mounts)
        with ctx as checkpoint_runner:
            try:
                checkpoint_runner.run_task(task, timeout=ANALYSIS_TIMEOUT)
            except ConfigurationTaskError as e:
                previous_error = previous_errors.get(task)
                if previous_error is None:
                    previous_errors[task] = e
                    resolved_errors[(task, e)] = idx, -1
                elif previous_error != e:
                    previous_key = (task, previous_error)
                    start, _ = resolved_errors[previous_key]
                    resolved_errors[previous_key] = start, idx

    # Enter a Docker runner context for the system.
    system = tasks[0].system
    labels = {'synth': 'true', 'synth.task': 'configuration script analysis'}
    with ExitStack() as stack:
        runner = stack.enter_context(get_runner(system, mounts=mounts))
        temp_dir = stack.enter_context(TemporaryDirectory())
        stack.enter_context(cleanup_images(runner.client, labels))
        stack.enter_context(logger.indent())

        # Run the setup script if provided.
        if setup:
            width = math.ceil(math.log(len(setup.tasks), 10))
            task_str = '\n'.join(
                f'    {idx:{width}.0f} {task}'
                for idx, task in enumerate(setup.tasks)
            )
            logger.info(f'Running setup script:\n{task_str}')
            for task in setup.tasks:
                try:
                    runner.run_task(task, timeout=ANALYSIS_TIMEOUT)
                except ConfigurationTaskError:
                    logger.exception(
                        'Configuration script failed during setup.'
                    )
                    raise

        # Commit the original container as an image.
        previous_image: Image = runner.container.commit(
            repository='synth/analysis-base-image',
            conf={'Labels': labels},
        )

        # Perform an initial test of all configuration tasks.
        logger.info('Performing initial pass to test for failures.')
        for task in tasks:
            logger.verbose(f'Running task: `{task}`')
            run_subsequent(system, previous_image.id, task, 0)

        # Run through the entire configuration script.
        logger.info('Running through configuration script.')
        failed_at_task = None
        configuration_task_error = None
        for i in range(len(tasks)):

            # Get the task at i.
            task = tasks[i]
            logger.info(f'Testing `{task}`.')

            # Run the task. At this point all prior tasks have succeeded. If
            # the current task fails, then there is an error in the
            # configuration script. Log the error and break. We'll use partial
            # results from everything up until this point. If it succeeds and
            # there was a previous error, then record the ending index of the
            # tasks that are needed to resolve the error.
            try:
                runner.run_task(task, timeout=ANALYSIS_TIMEOUT)
            except ConfigurationTaskError as e:
                logger.exception('Configuration script failed with error.')
                failed_at_task = task
                configuration_task_error = e
                break

            # If an error occurred previously, mark it as resolved. Otherwise,
            # no error occurred for this task. Add an entry with no error or
            # resolving tasks for the output.
            previous_error = previous_errors.get(task)
            if previous_error is not None:
                previous_key = (task, previous_error)
                start, _ = resolved_errors[previous_key]
                resolved_errors[previous_key] = start, i
            else:
                resolved_errors[(task, None)] = None

            # Commit the result as a new image.
            image: Image = runner.container.commit(
                repository=f'synth/analysis-{i}',
                conf={'Labels': labels},
            )

            # Get changes made by running the task.
            changes[task] = diff_images(
                previous_image.tags[0],
                image.tags[0],
                cache_dir=temp_dir,
            )

            # Remove the previous image and save the current image as previous.
            logger.verbose(f'Removing cached image: `{previous_image.id}`')
            runner.client.images.remove(previous_image.id)
            previous_image = image

            # Test subsequent tasks for errors.
            # Don't test the next one (i + 1) for errors. It will be tested by
            # the next iteration, and if the error hasn't been resolved by that
            # point then the script is considered invalid.
            if i + 2 < len(tasks):
                logger.verbose('Testing subsequent tasks for failures.')
                for j in range(i + 2, len(tasks)):
                    other_task = tasks[j]
                    run_subsequent(system, image.id, other_task, i)

        # Remove the last image.
        logger.verbose(f'Removing cached image: `{previous_image.id}`')
        runner.client.images.remove(previous_image.id)

    # Convert all tasks to configuration tasks with their discovered changes.
    tasks_with_changes = [
        ConfigurationTask(
            system=task.system,
            executable=task.executable,
            arguments=task.arguments,
            changes=changes.get(task, frozenset()),
        )
        for task in tasks
    ]
    task_change_lookup = {
        task: task_with_changes
        for task, task_with_changes in zip(tasks, tasks_with_changes)
    }

    # Process all results.
    result = []
    for (task, error), indices in resolved_errors.items():
        if indices is not None and indices[1] == -1:
            continue
        else:
            if indices is None:
                resolving_tasks = None
            else:
                start, end = indices
                resolving_tasks = tuple(tasks_with_changes[start:end])

            task_with_changes = task_change_lookup[task]
            result.append((None, task_with_changes, error, resolving_tasks))

    return AnalysisResult(
        success=(failed_at_task is None),
        tasks=frozenset(result),
        failed_at_task=failed_at_task,
        configuration_task_error=configuration_task_error,
    )


def analyze_and_record(path: Path,
                       context: Path,
                       setup_path: Optional[Path] = None,
                       parse_only: bool = False,
                       ) -> AnalysisResult:
    """Parse and analyze a configurations script, then record the results.

    Parameters
    ----------
    path : Path
        Path to a supported configurations script type.
    context : Path
        The context directory for the configuration script. This should be the
        path to the directory it expects to be run from. For Dockerfiles, this
        is the Docker build context.
    setup_path : Optional[Path]
        Path to a setup script. If provided tasks from this script will be
        run before analysis begins.
    parse_only : bool
        If true the configuration script will be parsed and the tasks returned
        without execution. Execution results will not be recorded.

    Returns
    -------
    AnalysisResult
        Data from running analysis.
    """
    if setup_path:
        parse = get_parser(setup_path)
        logger.info(f'Parsing setup `{setup_path}` with `{parse.__name__}`.')
        setup_result = parse(setup_path, context=context)
    else:
        setup_result = None

    parse = get_parser(path)
    logger.info(f'Parsing `{path}` with `{parse.__name__}`.')
    result = parse(path, context=context)

    if parse_only:
        return AnalysisResult(
            success=True,
            tasks=frozenset((None, task, None, None) for task in result.tasks),
        )

    logger.info('Analyzing configuration script.')
    analysis_result = analyze_configuration_script(
        context,
        result,
        setup=setup_result,
    )

    logger.info('Inserting task executions into the knowledge base.')
    insert_task_executions(analysis_result.tasks)

    return analysis_result


def _parse_curated_task(item: dict[str, Any]) -> ConfigurationTask:
    """Parse a configuration task definition in an analysis script.

    Parameters
    ----------
    item : dict[str, Any]
        Analysis script item containing the task definition.

    Raises
    ------
    ValueError
        Raised if a task cannot be parsed.

    Returns
    -------
    ConfigurationTask
        Parsed configuration task
    """
    if 'shell' in item:
        return parse_shell_script(item['shell']).tasks[0]
    elif 'ansible' in item:
        ansible_task = item['ansible']
        return ConfigurationTask(
            system=ConfigurationSystem.ANSIBLE,
            executable=ansible_task['executable'],
            arguments=frozendict(ansible_task['arguments']),
            changes=frozenset(),
        )
    else:
        raise ValueError(f'Unable to parse curated task item `{item}`.')


def process_analysis_script(path: Path) -> AnalysisResult:
    """Process a synth analysis script.

    This is similar to ``analyze_configuration_script``, but is designed to
    work for Synth's custom analysis script format. Analysis scripts are a JSON
    or YAML file that contain a list of tasks. Each task must have a ``shell``
    attribute that contains a shell command to run. Tasks may have a boolean
    ``analyze`` attribute that specifies if the task's changes should be
    analyzed and recorded (default False). Tasks may also have an optional
    ``reduce`` attribute that specifies additional tasks to use in reducing
    the change set. Only changes held in common with the reduction set will
    be retained.

    Parameters
    ----------
    path : Path
        Path to the analysis script.

    Returns
    -------
    AnalysisResult
        Results from analysis.
    """
    logger.info(f'Running analysis script `{path}`.')

    # Load the analysis script.
    script = yaml.safe_load(path.read_text())

    # Exit early if the script is empty.
    if not script:
        logger.info('Script is empty.')
        return AnalysisResult(success=True, tasks=frozenset())

    # Parse the script for tasks.
    tasks = []
    for item in script:
        task = _parse_curated_task(item)
        reduction_list = [
            _parse_curated_task(reduction_task)
            for reduction_task in item.get('reduce', [])
        ]
        tasks.append({
            'task': task,
            'level': item.get('level', 1),
            'analyze': item.get('analyze', False),
            'reduce': reduction_list,
        })

    # Record the previous error encountered for all tasks and the range of
    # tasks for which that error was present.
    previous_errors: dict[ConfigurationTask, ConfigurationTaskError] = {}
    resolved_errors: dict[tuple[int,
                                ConfigurationTask,
                                Optional[ConfigurationTaskError]],
                          Optional[tuple[int, int]]] = {}
    changes: dict[ConfigurationTask, frozenset[ConfigurationChange]] = {}

    def run_subsequent(system: ConfigurationSystem,
                       image: str,
                       level: int,
                       task: ConfigurationTask,
                       idx: int):
        """Run a subsequent configuration task.

        This is a helper method to record error results.

        Parameters
        ----------
        system : ConfigurationSystem
            The configuration system to use.
        image : str
            The Docker runner image tag.
        level : int
            The task level.
        task : ConfigurationTask
            The configuration task to run.
        idx : int
            The current index.
        """
        ctx = get_runner(system, image=image)
        with ctx as checkpoint_runner:
            try:
                checkpoint_runner.run_task(task, timeout=ANALYSIS_TIMEOUT)
            except ConfigurationTaskError as e:
                previous_error = previous_errors.get(task)
                if previous_error is None:
                    previous_errors[task] = e
                    resolved_errors[(level, task, e)] = idx, -1
                elif previous_error != e:
                    previous_key = (level, task, previous_error)
                    start, _ = resolved_errors[previous_key]
                    resolved_errors[previous_key] = start, idx

    # Enter context for analysis.
    system = tasks[0]['task'].system
    labels = {'synth': 'true', 'synth.task': 'configuration script analysis'}
    with ExitStack() as stack:
        runner = stack.enter_context(get_runner(system))
        temp_dir = stack.enter_context(TemporaryDirectory())
        stack.enter_context(cleanup_images(runner.client, labels))
        stack.enter_context(logger.indent())

        # Commit the original container as an image.
        logger.verbose('Committing image synth/analysis-base-image.')
        previous_image: Image = runner.container.commit(
            repository='synth/analysis-base-image',
            conf={'Labels': labels},
        )

        # Perform an initial test of all analyzed tasks.
        logger.info('Performing initial pass to test for failures.')
        for task in tasks:
            if task['analyze']:
                logger.verbose(f'Running task: `{task["task"]}`')
                run_subsequent(
                    system, previous_image.id, task['level'], task['task'], 0
                )

        # Run through the entire configuration script.
        logger.info('Running through configuration script.')
        failed_at_task = None
        configuration_task_error = None
        for i in range(len(tasks)):

            # Get the task at i.
            task = tasks[i]
            logger.info(f'Running `{task["task"]}`.')

            # Run the task. At this point all prior tasks have succeeded. If
            # the current task fails, then there is an error in the
            # configuration script. Log the error and break. We'll use partial
            # results from everything up until this point. If it succeeds and
            # there was a previous error, then record the ending index of the
            # tasks that are needed to resolve the error.
            try:
                runner.run_task(task["task"], timeout=ANALYSIS_TIMEOUT)
            except ConfigurationTaskError as e:
                logger.exception('Configuration script failed with error.')
                failed_at_task = task["task"]
                configuration_task_error = e
                break

            # Commit the result as a new image.
            logger.verbose(f'Committing image synth/analysis-{i}')
            image: Image = runner.container.commit(
                repository=f'synth/analysis-{i}',
                conf={'Labels': labels},
            )

            # If this task is being analyzed, process the errors and diff.
            if task['analyze']:
                # If an error occurred previously, mark it as resolved.
                # Otherwise, no error occurred for this task. Add an entry
                # with no error or resolving tasks for the output.
                previous_error = previous_errors.get(task['task'])
                if previous_error is not None:
                    previous_key = (
                        task['level'], task['task'], previous_error
                    )
                    start, _ = resolved_errors[previous_key]
                    resolved_errors[previous_key] = start, i
                else:
                    resolved_errors[(task['level'], task['task'], None)] = None

                # Get changes made by running the task.
                task_changes = {
                    c.from_arguments(task['task'].configuration_task_arguments)
                    for c in diff_images(
                        previous_image.tags[0],
                        image.tags[0],
                        cache_dir=temp_dir,
                    )
                }
                logger.debug(f'Found `{len(task_changes)}` task changes.')

                # Process reduction tasks.
                if task['reduce']:
                    logger.info('Processing reduction tasks.')
                for r_idx, reduction_task in enumerate(task['reduce']):

                    # Create a new runner context based on the previous image.
                    ctx = get_runner(
                        reduction_task.system,
                        image=previous_image.id,
                    )
                    with ctx as reduction_runner:

                        # Run the reduction task. Skip the task if it fails.
                        try:
                            logger.verbose(
                                f'Running reduction task `{reduction_task}`.'
                            )
                            reduction_runner.run_task(
                                reduction_task,
                                timeout=ANALYSIS_TIMEOUT,
                            )
                        except ConfigurationTaskError:
                            logger.exception(
                                'Reduction task failed, skipping.'
                            )
                            continue

                        # Commit the results of the reduction task as a new
                        # image.
                        logger.verbose(
                            f'Committing image '
                            f'synth/analysis-{i}-reduction-{r_idx}'
                        )
                        reduction_image = (
                            reduction_runner
                            .container
                            .commit(
                                repository=f'synth/analysis-{i}'
                                           f'-reduction-{r_idx}',
                                conf={'Labels': labels},
                            )
                        )

                        # Get the changes made by the reduction task.
                        reduction_changes = {
                            c.from_arguments(
                                reduction_task.configuration_task_arguments,
                            )
                            for c in diff_images(
                                previous_image.tags[0],
                                reduction_image.tags[0],
                                cache_dir=temp_dir,
                            )
                        }
                        logger.debug(
                            f'Found `{len(reduction_changes)}` '
                            f'reduction changes.'
                        )

                        try:
                            # If the reduction changes are exactly the same
                            # as the original task's changes, then computing
                            # the intersection will not result in a change.
                            if reduction_changes == task_changes:
                                logger.debug(
                                    'Reduction task changes exactly match the '
                                    'primary task changes, skipping.'
                                )
                                continue

                            # Compute the change intersection with mapping.
                            logger.verbose('Computing change intersection.')
                            _, task_intersection, _ = (
                                ConfigurationChange
                                .change_intersection(
                                    reduction_changes,
                                    task_changes,
                                )
                            )

                            # If there was no intersection, then this reduction
                            # is not helpful.
                            if not task_intersection:
                                logger.debug('No tasks in intersection.')
                                continue

                            # Preserve only the changes that were in the
                            # intersection with the reduction task.
                            logger.debug('Subsetting task changes.')
                            task_changes &= task_intersection
                            logger.debug(
                                f'`{len(task_changes)}` changes after '
                                f'subsetting.'
                            )

                        finally:
                            logger.verbose(
                                f'Removing cached image: '
                                f'`{reduction_image.id}`'
                            )
                            runner.client.images.remove(reduction_image.id)

                changes[task['task']] = frozenset(task_changes)

                # Test subsequent tasks for errors.
                # Don't test the next one (i + 1) for errors. It will be tested
                # by the next iteration, and if the error hasn't been resolved
                # by that point then the script is considered invalid.
                if i + 2 < len(tasks):
                    logger.verbose('Testing subsequent tasks for failures.')
                    for j in range(i + 2, len(tasks)):
                        other_task = tasks[j]
                        if other_task['analyze']:
                            run_subsequent(
                                system,
                                image.id,
                                other_task['level'],
                                other_task['task'],
                                i,
                            )

            # Remove the previous image and save the current image as previous.
            logger.verbose(f'Removing cached image: `{previous_image.id}`')
            runner.client.images.remove(previous_image.id)
            previous_image = image

        # Remove the last image.
        logger.verbose(f'Removing cached image: `{previous_image.id}`')
        runner.client.images.remove(previous_image.id)

    # Convert all tasks to configuration tasks with their discovered changes.
    task_change_lookup = {
        task['task']: ConfigurationTask(
            system=task['task'].system,
            executable=task['task'].executable,
            arguments=task['task'].arguments,
            changes=changes.get(task['task'], frozenset()),
        )
        for task in tasks
        if task['analyze']
    }

    # Process all results.
    result = []
    for (level, task, error), indices in resolved_errors.items():
        if indices is not None and indices[1] == -1:
            continue
        else:
            if indices is None:
                resolving_tasks = None
            else:
                start, end = indices
                resolving_tasks = tuple(map(
                    lambda t: t['task'],
                    tasks[start:end],
                ))

            task_with_changes = task_change_lookup[task]
            result.append((level, task_with_changes, error, resolving_tasks))

    analysis_result = AnalysisResult(
        success=(failed_at_task is None),
        tasks=frozenset(result),
        failed_at_task=failed_at_task,
        configuration_task_error=configuration_task_error,
    )

    logger.info('Inserting task executions into the knowledge base.')
    insert_task_executions(analysis_result.tasks)

    return analysis_result
