"""Synth dataset preparation."""


# Imports.
import shutil
from hashlib import sha1
from pathlib import Path

import pandas
from docker import client
from docker.errors import BuildError
from docker.models.images import Image
from pandas import DataFrame

from synth.datasets import (
    ANSIBLE_CURATED_ANALYSIS_SCRIPTS_DATA_DIR,
    ANSIBLE_CURATED_ANALYSIS_SCRIPTS_METADATA_DIR,
    DOCKER_CURATED_DOCKERFILES_DATA_DIR,
    DOCKER_CURATED_DOCKERFILES_METADATA_DIR,
    SHELL_CURATED_ANALYSIS_SCRIPTS_DATA_DIR,
    SHELL_CURATED_ANALYSIS_SCRIPTS_METADATA_DIR,
)
from synth.logging import logger
from synth.synthesis.classes import ConfigurationSystem, ConfigurationTask
from synth.synthesis.configuration_scripts.docker import parse_dockerfile
from synth.synthesis.docker import ShellTaskError, ShellTaskRunner
from synth.util.shell import join
from synth.util.timeout import Timeout


# Constants
RANDOM_STATE = 22397
TRAINING_SET_FRAC = 0.2
TASK_TIMEOUT_SECONDS = 10
BUILD_TIMEOUT_SECONDS = 10_800  # 3 hours.


def prepare_curated_dockerfiles():
    """Prepare curated Dockerfiles."""
    shutil.rmtree(DOCKER_CURATED_DOCKERFILES_DATA_DIR)
    for index_path in DOCKER_CURATED_DOCKERFILES_METADATA_DIR.glob('*.csv'):
        dest_dir = DOCKER_CURATED_DOCKERFILES_DATA_DIR / index_path.stem
        shutil.copytree(index_path.parent / index_path.stem, dest_dir)
        shutil.copy(index_path, dest_dir / 'index.csv')


def prepare_curated_analysis_scripts():
    """Prepare curated shell scripts."""
    shutil.rmtree(ANSIBLE_CURATED_ANALYSIS_SCRIPTS_DATA_DIR)
    index_paths = ANSIBLE_CURATED_ANALYSIS_SCRIPTS_METADATA_DIR.glob('*.csv')
    for index_path in index_paths:
        dest_dir = ANSIBLE_CURATED_ANALYSIS_SCRIPTS_DATA_DIR / index_path.stem
        shutil.copytree(index_path.parent / index_path.stem, dest_dir)
        shutil.copy(index_path, dest_dir / 'index.csv')

    shutil.rmtree(SHELL_CURATED_ANALYSIS_SCRIPTS_DATA_DIR)
    index_paths = SHELL_CURATED_ANALYSIS_SCRIPTS_METADATA_DIR.glob('*.csv')
    for index_path in index_paths:
        dest_dir = SHELL_CURATED_ANALYSIS_SCRIPTS_DATA_DIR / index_path.stem
        shutil.copytree(index_path.parent / index_path.stem, dest_dir)
        shutil.copy(index_path, dest_dir / 'index.csv')


def prepare_dockerfiles(metadata: dict[str, Path],
                        data_dir: Path):
    """Prepare Dockerfiles data for experiments.

    Datasets must have been downloaded prior to running preparation. This
    method will filter the Dockerfiles to those that:

    1. Parse.
    2. Build.
    3. Can't run the ENTRYPOINT + CMD in the base image.
    4. Can run the ENTRYPOINT + CMD in the configured image.

    It will then split the remaining Dockerfiles 20/80 into training and test
    datasets.

    Parameters
    ----------
    metadata : dict[str, Path]
        Metadata dictionary for the dataset.
    data_dir : Path
        Path to the dataset's data directory.
    """
    # Get a docker client.
    docker_client = client.from_env()
    docker_client.info()

    logger.info('Preparing Dockerfile datasets.')
    for dataset in sorted(metadata.keys()):
        logger.info(f'Preparing dataset `{dataset}`.')

        # Get the dataset index file path.
        index_path = data_dir / dataset / 'index.csv'

        # Error if the file is missing.
        if not index_path.is_file():
            raise FileNotFoundError(
                f'Cannot find the Dockerfiles index at {index_path}. Did you '
                f'run the download or unpack step?'
            )

        # Load the index.
        index: DataFrame = pandas.read_csv(index_path)
        index.sort_values(['repo_name', 'ref', 'path'])

        # Find all rows of the index that are valid for the experiment.
        # To be valid the Dockerfiles must parse, build, and have a default
        # ENTRYPOINT + CMD that fails in the base image but executes
        # successfully in the configured image.
        valid_rows = []
        failed_rows = []
        num_dockerfiles = len(index)
        for idx, row in index.iterrows():
            repo_name = row['repo_name']
            ref = row['ref']
            path = row['path']
            repo_dir = row['repo_dir']

            logger.verbose(
                f'({idx + 1}/{num_dockerfiles}) Testing `{path}` in '
                f'`{repo_name}` at ref `{ref}`.'
            )

            repo_dir_path = data_dir / dataset / repo_dir
            dockerfile_path = repo_dir_path / path

            # Test the Dockerfile.
            stage = 'parsing'
            try:
                with logger.indent():
                    # Parse
                    logger.verbose('Parsing.')
                    parse_dockerfile(dockerfile_path)

                    # Build. Try to use the Dockerfile directory as the build
                    # context first. If that fails, keep trying ancestors until
                    # reaching the repo root directory. If that fails, mark
                    # the build as failed.
                    stage = 'building'
                    sha = sha1(f'{repo_name}/{path}'.encode())  # noqa: S303
                    tag = f'synth-experiment/{sha.hexdigest()}'
                    dockerfile_ancestors = [
                        v
                        for v in dockerfile_path.parents
                        if v.is_relative_to(repo_dir_path)
                    ]
                    image: Image
                    logger.verbose(f'Building image `{tag}`.')
                    with Timeout(seconds=BUILD_TIMEOUT_SECONDS,
                                 error_message=f'Docker build timed out at '
                                               f'{BUILD_TIMEOUT_SECONDS} '
                                               f'seconds.'):
                        for ancestor in dockerfile_ancestors:
                            try:
                                logger.verbose(
                                    f'Attempting build using the build '
                                    f'context `{ancestor}`.'
                                )
                                image, _ = docker_client.images.build(
                                    path=str(ancestor),
                                    dockerfile=str(dockerfile_path),
                                    tag=tag,
                                    network_mode='synth_default',
                                )
                            except BuildError:
                                if ancestor == repo_dir_path:
                                    raise
                            else:
                                context_path = ancestor
                                logger.verbose(
                                    f'Build succeeded with context path '
                                    f'`{context_path}`.'
                                )
                                break

                    # Parse again using the context directory to resolve
                    # mounts.
                    logger.verbose(
                        'Re-parsing with build context directory to resolve '
                        'mounts.'
                    )
                    stage = 'parsing-mounts'
                    result = parse_dockerfile(
                        dockerfile_path,
                        context=context_path,
                    )

                    # Get the dfault command.
                    data = docker_client.api.inspect_image(image.id)
                    config = data['Config']
                    entrypoint = config['Entrypoint'] or []
                    cmd = config['Cmd'] or []
                    default_command = tuple(entrypoint + cmd)
                    default_task = ConfigurationTask(
                        system=ConfigurationSystem.SHELL,
                        executable=default_command[0],
                        arguments=default_command[1:],
                        changes=frozenset(),
                    )

                    # Run the default command in the base image. Expect it to
                    # fail.
                    stage = 'base-image-exec'
                    logger.verbose(
                        f'Running command `{join(default_command)}` in the '
                        f'base image.'
                    )
                    with ShellTaskRunner(image=result.base_image,
                                         is_runner_image=False,
                                         mounts=result.mounts) as runner:
                        try:
                            runner.run_task(
                                default_task,
                                timeout=TASK_TIMEOUT_SECONDS,
                            )
                        except ShellTaskError:
                            pass  # The task failed, proceed with preparation.
                        else:
                            raise Exception(
                                'The default command did not fail in the '
                                'base image.'
                            )

                    # Run the default command in the configured image. Expect
                    # it to succeed.
                    stage = 'configured-image-exec'
                    logger.verbose(
                        f'Running command: `{join(default_command)}`.'
                    )
                    with ShellTaskRunner(image=tag,
                                         is_runner_image=False,
                                         mounts=result.mounts,
                                         ) as runner:
                        try:
                            runner.run_task(
                                default_task,
                                timeout=TASK_TIMEOUT_SECONDS,
                            )
                        except TimeoutError:
                            pass  # Timeouts are considered a success.
            except Exception as e:  # noqa: B902
                logger.exception(
                    'Testing failed with error, will exclude from training '
                    'or test sets.'
                )
                failed_rows.append((idx, stage, str(e)))
                continue
            else:
                logger.verbose(
                    f'Testing succeeded with context path `{context_path}`.'
                )
                valid_rows.append((idx, context_path, result.mounts))

        # Create an index of only valid rows and then split into test and
        # training datasets.
        valid_index: DataFrame = DataFrame(
            data=[row[1:] for row in valid_rows],
            index=[row[0] for row in valid_rows],
            columns=['context_path', 'mounts'],
        )
        valid_index: DataFrame = index.join(valid_index, how='inner')

        training_set: DataFrame = valid_index.sample(
            frac=TRAINING_SET_FRAC,
            random_state=RANDOM_STATE,
        )
        test_set: DataFrame = valid_index.drop(training_set.index)

        failures: DataFrame = DataFrame(
            data=[row[1:] for row in failed_rows],
            index=[row[0] for row in failed_rows],
            columns=['failed_at_stage', 'error_message']
        )
        failures = index.join(failures, how='inner')

        # Write the training set.
        training_set_index_path = data_dir / dataset / 'training_set.csv'
        training_set.to_csv(training_set_index_path, index=False)

        # Write the test set.
        test_set_index_path = data_dir / dataset / 'test_set.csv'
        test_set.to_csv(test_set_index_path, index=False)

        # Write the failures.
        failure_path = data_dir / dataset / 'failures.csv'
        failures.to_csv(failure_path, index=False)
