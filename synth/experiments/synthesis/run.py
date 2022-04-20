"""Run synthesis experiments."""


# Imports.
import json
from collections.abc import Sequence
from contextlib import closing
from dataclasses import dataclass
from multiprocessing import (
    Pipe,
    Process,
)
from multiprocessing.connection import Connection
from pathlib import Path
from time import time

from docker import client
from docker.client import DockerClient
from docker.types import Mount
from pandas import DataFrame

from synth.logging import logger, set_level
from synth.synthesis import synthesize_configuration_tasks
from synth.synthesis.classes import (
    ConfigurationSystem,
    ConfigurationTask,
    ConfigurationTaskError,
    ShellTaskError,
)
from synth.synthesis.configuration_scripts import get_default_name, get_writer
from synth.synthesis.configuration_scripts.docker import parse_dockerfile
from synth.synthesis.docker import diff_images, get_runner, ShellTaskRunner
from synth.synthesis.serialization import SynthJSONEncoder
from synth.util.timeout import Timeout


# Constants
ENTRYPOINT_TASK_TIMEOUT = 60  # 1 minute.
CONFIGURATION_TASK_TIMEOUT = 3600  # 1 hour.


@dataclass(frozen=True)
class SynthesisExperimentResult:
    """Results from executing a synthesis experiment.

    Attributes
    ----------
    base_image_exec : bool
        True iff executing the default entrypoint + command succeeds in the
        base image without configuration.
    base_image_exec_result_path : Path
        Path to the base image execution output.
    configured_image_exec : bool
        True iff executing the default entrypoint + command succeeds in the
        configured image.
    configured_image_exec_result_path : Path
        Path to the configured image execution output.
    synthesized_image_exec : bool
        True iff executing the default entrypoint + command succeeds in the
        synthesized image.
    synthesized_image_exec_result_path : Path
        Path to the synthesized image execution output.
    jaccard_coefficient : float
        The intersection over union of the configuration changes in the
        configured and synthesized images.
    output_dir : Path
        Path to the output directory containing all cached output.
    configured_image_diff_path : Path
        Path to a JSON file containing the diff between the base and configured
        images.
    synthesized_image_diff_path : Path
        Path to a JSON file containing the diff between the base and
        synthesized images.
    synthesized_configuration_script_path : Path
        Path to the synthesized configuration script.
    synthesis_time : float
        Length of time (in seconds) taken for synthesis.
    """

    base_image_exec: bool
    base_image_exec_result_path: Path
    configured_image_exec: bool
    configured_image_exec_result_path: Path
    synthesized_image_exec: bool
    synthesized_image_exec_result_path: Path
    jaccard_coefficient: float
    output_dir: Path
    configured_image_diff_path: Path
    synthesized_image_diff_path: Path
    synthesized_configuration_tasks_path: Path
    synthesized_configuration_script_path: Path
    synthesis_time: float


def _run_synthesis(log_level: int,
                   conn: Connection,
                   system: ConfigurationSystem,
                   image: str,
                   base_image: str,
                   is_runner_image: bool,
                   mounts: list[Mount]):
    """Run synthesis multiprocess.

    Parameters
    ----------
    log_level : int
        Effective logging level to use.
    conn : Pipe
        Pipe to send the results back to the main process.
    system : ConfigurationSystem
        Desired configuration system.
    image : str
        The configured Docker image. This will be used to compute a diff.
    base_image : str
        The base Docker image. If provided this image will be used for
        ordering.
    is_runner_image : bool
        Whether the base image is a Synth runner image.
    mounts : list[Mount]
        Docker mounts to use during search.
    """
    import signal
    from types import FrameType

    def _terminate(signum: int, frame: FrameType):
        logger.debug(f'Terminating with exit code: `{signum}`.')
        exit(signum)
    signal.signal(signal.SIGTERM, _terminate)

    set_level(log_level)
    tasks = synthesize_configuration_tasks(
        system=system,
        image=image,
        base_image=base_image,
        is_runner_image=is_runner_image,
        mounts=mounts,
    )
    logger.debug('Sending configuration tasks to main process.')
    conn.send(tasks)
    logger.debug('Done sending. Closing connection.')
    conn.close()
    logger.debug('Closed.')


def run_docker_synthesis_experiment(dockerfile: Path,
                                    context: Path,
                                    mounts: Sequence[Mount],
                                    tag: str,
                                    system: ConfigurationSystem,
                                    output: Path,
                                    synthesis_timeout: int = 9000,
                                    ) -> SynthesisExperimentResult:
    """Run a Docker synthesis experiment.

    This experiment will build a Docker image and then test synthesizing a
    configuration script in the target system.

    Parameters
    ----------
    dockerfile : Path
        Path to a Dockerfile to use for synthesis.
    context : Path
        Path to the Docker build context.
    mounts : Sequence[Mount]
        Mounts required for Docker tasks.
    tag : str
        Tag that will be used for the built Docker image.
    system : ConfigurationSystem
        The target configuration system. The synthesized configuration script
        will be in this system.
    output : Path
        Output directory for cached experiment metadata.
    synthesis_timeout : int
        Timeout in seconds for configuration synthesis. Default is 9000
        (2.5 hours).

    Returns
    -------
    SynthesisExperimentResult
        Experiment results.
    """
    logger.info(f'Running Docker synthesis experiment for `{dockerfile}`.')
    logger.verbose(f'Experiment metadata will be output to `{output}`.')

    if mounts:
        mounts = list(mounts)

    # Create a Docker client and verify the connection.
    docker_client: DockerClient = client.from_env()
    docker_client.info()

    # Parse the configuration script.
    logger.verbose('Parsing the Dockerfile.')
    result = parse_dockerfile(dockerfile, context=context)

    # Verify the output directory exists.
    output.mkdir(exist_ok=True, parents=True)

    # Write the configuration script metadata to the output directory.
    logger.verbose('Writing script metadata.')
    script_metadata_path = output / 'script_metadata.csv'
    script_metadata = (
        DataFrame(
            [dockerfile, context, mounts, system],
            index=['dockerfile', 'context', 'mounts', 'system'],
            columns=['value'],
        )
        .rename_axis('attribute')
    )
    script_metadata.to_csv(script_metadata_path)

    # Build the Docker image.
    logger.verbose('Building the configured image based on the Dockerfile.')
    image, _ = docker_client.images.build(
        dockerfile=str(dockerfile),
        tag=tag,
        path=str(context),
        network_mode='synth_default',
    )

    # Get the full command that would be executed when a container is run.
    # This is the image entrypoint + the image command.
    data = docker_client.api.inspect_image(image.id)
    config = data['Config']
    entrypoint = config['Entrypoint'] or []
    cmd = config['Cmd'] or []
    command = tuple(entrypoint + cmd)
    command_task = ConfigurationTask(
        system=ConfigurationSystem.SHELL,
        executable=command[0],
        arguments=command[1:],
        changes=frozenset(),
    )
    logger.verbose(f'The default command (ENTRYPOINT + CMD) is `{command}`.')

    # Check if the command executes successfully in the base image.
    logger.verbose('Testing the default command in the base image.')
    with ShellTaskRunner(image=result.base_image,
                         is_runner_image=False,
                         mounts=result.mounts) as runner:
        base_image_exec_result_path = output / 'base_image_exec_result.json'
        try:
            run_result = runner.run_task(
                command_task,
                timeout=ENTRYPOINT_TASK_TIMEOUT,
            )
        except ShellTaskError as e:
            base_image_exec = False
            base_image_exec_result_path.write_text(json.dumps(
                e,
                cls=SynthJSONEncoder,
            ))
        except TimeoutError:
            base_image_exec = True
            base_image_exec_result_path.write_text(json.dumps({
                'type': 'TimeoutError',
                'value': 'Timeout',
            }))
        else:
            base_image_exec = True
            base_image_exec_result_path.write_text(json.dumps(
                run_result,
                cls=SynthJSONEncoder,
            ))

    if base_image_exec:
        logger.verbose('The default command succeeded in the base image.')
    else:
        logger.debug('The default command failed in the base image.')

    # Check if the command executes successfully in the configured image.
    logger.verbose('Testing the default command in the configured image.')
    with ShellTaskRunner(image=tag,
                         is_runner_image=False,
                         mounts=result.mounts) as runner:
        configured_image_exec_result_path = (
            output / 'configured_image_exec_result.json'
        )
        try:
            run_result = runner.run_task(
                command_task,
                timeout=ENTRYPOINT_TASK_TIMEOUT,
            )
        except ShellTaskError as e:
            configured_image_exec = False
            configured_image_exec_result_path.write_text(json.dumps(
                e,
                cls=SynthJSONEncoder,
            ))
        except TimeoutError:
            configured_image_exec = True
            configured_image_exec_result_path.write_text(json.dumps({
                'type': 'TimeoutError',
                'value': 'Timeout',
            }))
        else:
            configured_image_exec = True
            configured_image_exec_result_path.write_text(json.dumps(
                run_result,
                cls=SynthJSONEncoder,
            ))

    if configured_image_exec:
        logger.verbose(
            'The default command succeeded in the configured image.'
        )
    else:
        logger.verbose('The default command failed in the configured image.')

    # Diff the configured image.
    logger.verbose('Diffing the configured image.')
    configured_image_diff = diff_images(
        result.base_image,
        tag,
        mounts=mounts,
    )
    configured_image_diff_path = output / 'configured_image_diff.json'
    configured_image_diff_path.write_text(json.dumps(
        configured_image_diff,
        cls=SynthJSONEncoder,
    ))

    # Synthesize a configuration script in the desired system.
    # Running multiprocess allows us to timeout using join() while allowing the
    # synthesis process to use the signal based Timeout() utility.
    logger.verbose(f'Synthesizing a configuration script for `{system}`.')
    parent_conn, child_conn = Pipe()
    synthesis_process = Process(
        target=_run_synthesis,
        args=(),
        kwargs={
            'log_level': logger.getEffectiveLevel(),
            'conn': child_conn,
            'system': system,
            'image': tag,
            'base_image': result.base_image,
            'is_runner_image': False,
            'mounts': mounts,
        },
        daemon=False,
    )
    with closing(parent_conn), closing(child_conn), closing(synthesis_process):
        try:
            with Timeout(seconds=synthesis_timeout):
                synthesis_start_time = time()
                synthesis_process.start()
                synthesized_configuration_tasks = parent_conn.recv()
                synthesis_process.join()
                synthesis_end_time = time()
        except TimeoutError:
            logger.debug('Synthesis timed out. Terminating synthesis process.')
            synthesis_process.terminate()
            synthesis_process.join(timeout=180)  # 3 minutes.
            if synthesis_process.exitcode is None:
                logger.debug(
                    'Termination timed out. Killing synthesis process.'
                )
                synthesis_process.kill()
                synthesis_process.join(timeout=60)  # 1 minute
            raise

    synthesized_configuration_tasks_path = (
        output / 'synthesized_configuration_tasks.json'
    )
    synthesized_configuration_tasks_path.write_text(json.dumps(
        synthesized_configuration_tasks,
        cls=SynthJSONEncoder,
    ))
    write = get_writer(system)
    synthesized_configuration_script = write(synthesized_configuration_tasks)
    synthesized_configuration_script_path = (
        output / get_default_name(system, suffix='synthesized')
    )
    synthesized_configuration_script_path.write_text(
        synthesized_configuration_script
    )

    # Test the synthesized configuration script.
    with get_runner(system=system,
                    image=result.base_image,
                    is_runner_image=False,
                    mounts=result.mounts) as runner:
        # Run the script against a clean image.
        logger.verbose('Running the synthesized configuration script.')
        try:
            for task in synthesized_configuration_tasks:
                runner.run_task(task, timeout=CONFIGURATION_TASK_TIMEOUT)
        except ConfigurationTaskError:
            logger.exception(
                'Encountered an exception while rebuilding the synthesized '
                'image from the synthesized configuration script. This should '
                'not happen and is likely a bug.'
            )
            raise

        # Commit the runner container as an image.
        synthesized_tag = (
            f'synth-synthesized/{tag.split("/", maxsplit=1)[1]}'
        )
        labels = {
            'synth': 'true',
            'synth.task': 'configuration script synthesis',
        }
        logger.verbose(
            f'Committing synthesized image with tag `{synthesized_tag}`.'
        )
        runner.container.commit(
            repository=synthesized_tag,
            conf={'Labels': labels},
        )

    # Diff the synthesized image.
    logger.verbose('Diffing the synthesized image.')
    synthesized_image_diff = diff_images(
        result.base_image,
        synthesized_tag,
        mounts=mounts,
    )
    synthesized_image_diff_path = output / 'synthesized_image_diff.json'
    synthesized_image_diff_path.write_text(json.dumps(
        synthesized_image_diff,
        cls=SynthJSONEncoder,
    ))

    # Test the default command.
    logger.verbose('Testing the default command in the synthesized image.')
    synthesized_image_exec_result_path = (
        output / 'synthesized_image_exec_result.json'
    )
    with ShellTaskRunner(image=synthesized_tag,
                         is_runner_image=False,
                         mounts=result.mounts) as runner:
        try:
            run_result = runner.run_task(
                command_task,
                timeout=ENTRYPOINT_TASK_TIMEOUT,
            )
        except ShellTaskError as e:
            synthesized_image_exec = False
            synthesized_image_exec_result_path.write_text(json.dumps(
                e,
                cls=SynthJSONEncoder,
            ))
        except TimeoutError:
            synthesized_image_exec = True
            synthesized_image_exec_result_path.write_text(json.dumps({
                'type': 'TimeoutError',
                'value': 'Timeout',
            }))
        else:
            synthesized_image_exec = True
            synthesized_image_exec_result_path.write_text(json.dumps(
                run_result,
                cls=SynthJSONEncoder,
            ))

    # Remove the image.
    docker_client.images.remove(synthesized_tag)

    # Compute the jaccard coefficient.
    jaccard_coefficient = (
        len(configured_image_diff & synthesized_image_diff)
        / len(configured_image_diff | configured_image_diff)
    )

    # Write and return experiment results.
    experiment_results_path = output / 'results.csv'
    experiment_results = (
        DataFrame(
            [
                base_image_exec,
                configured_image_exec,
                synthesized_image_exec,
                jaccard_coefficient,
                configured_image_diff_path,
                synthesized_image_diff_path,
                synthesized_configuration_tasks_path,
                synthesized_configuration_script_path,
            ],
            index=[
                'base_image_exec',
                'configured_image_exec',
                'synthesized_image_exec',
                'jaccard_coefficient',
                'configured_image_diff_path',
                'synthesized_image_diff_path',
                'synthesized_configuration_tasks_path',
                'synthesized_configuration_script_path',
            ],
            columns=['value'],
        )
        .rename_axis('attribute')
    )
    experiment_results.to_csv(experiment_results_path)

    return SynthesisExperimentResult(
        base_image_exec=base_image_exec,
        base_image_exec_result_path=base_image_exec_result_path,
        configured_image_exec=configured_image_exec,
        configured_image_exec_result_path=configured_image_exec_result_path,
        synthesized_image_exec=synthesized_image_exec,
        synthesized_image_exec_result_path=synthesized_image_exec_result_path,
        jaccard_coefficient=jaccard_coefficient,
        output_dir=output,
        configured_image_diff_path=configured_image_diff_path,
        synthesized_image_diff_path=synthesized_image_diff_path,
        synthesized_configuration_tasks_path=(
            synthesized_configuration_tasks_path
        ),
        synthesized_configuration_script_path=(
            synthesized_configuration_script_path
        ),
        synthesis_time=synthesis_end_time - synthesis_start_time,
    )
