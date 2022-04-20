"""Synth Debian Dockerfile experiments."""


# Imports.
import ast
import time
from hashlib import sha1
from pathlib import Path
from typing import Optional

import pandas
from docker.types import Mount
from pandas import DataFrame

from synth.datasets import (
    DOCKER_DEBIAN_UBUNTU_DOCKERFILES_DATA_DIR,
    DOCKER_DEBIAN_UBUNTU_DOCKERFILES_METADATA,
)
from synth.experiments.synthesis.run import run_docker_synthesis_experiment
from synth.logging import logger
from synth.paths import EXPERIMENTS_OUTPUT_DIR
from synth.synthesis.classes import ConfigurationSystem


# Constants.
AVAILABLE_DATASETS = sorted(DOCKER_DEBIAN_UBUNTU_DOCKERFILES_METADATA.keys())
DEFAULT_DATASET = 'sampled-deduplicated-debian-ubuntu-dockerfiles_2022-03-01'


def synthesize_configuration_scripts(dataset: str,
                                     system: ConfigurationSystem,
                                     include_training_set: bool,
                                     experiment_data_dir: Path,
                                     rerun_failed: Optional[Path] = None,
                                     ) -> DataFrame:
    """Run the synthesis experiment for a single dataset.

    Parameters
    ----------
    dataset : str
        Debian Dockerfile dataset to use.
    system : ConfigurationSystem
        Target configuration system.
    include_training_set : bool
        If True the training set will be included in the experiment. Otherwise
        only the test set will be used.
    experiment_data_dir : Path
        Output directory for experiment data.
    rerun_failed : Optional[Path]
        Path to a previous debian dockerfiles synthesis experiment run. If
        provided, results from the previous execution will be loaded and any
        images where synthesis did not finish or was not successful will be
        rerun.

    Raises
    ------
    ValueError
        Raised if the provided dataset does not exist.
    FileNotFoundError
        Raised if the test set does not exist or ``include_training_set=True``
        and the training set does not exist.

    Returns
    -------
    DataFrame
        DataFrame containing experiment results.
    """
    if dataset not in DOCKER_DEBIAN_UBUNTU_DOCKERFILES_METADATA:
        raise ValueError(
            f'Debian Dockerfile dataset `{dataset}` does not exist. '
            f'Available options are '
            f'`{sorted(DOCKER_DEBIAN_UBUNTU_DOCKERFILES_METADATA.keys())}`.'
        )

    logger.info(
        f'Running Synth Debian Dockerfiles experiment with:\n'
        f'    dataset              = `{dataset}`\n'
        f'    system               = `{system}`\n'
        f'    include_training_set = `{include_training_set}`\n'
        f'    rerun_failed         = `{rerun_failed}`'
    )

    # Load data.
    dataset_data_dir = DOCKER_DEBIAN_UBUNTU_DOCKERFILES_DATA_DIR / dataset
    if rerun_failed:
        logger.verbose('Loading old results set.')
        old_results_path = rerun_failed / 'results.csv'
        df: DataFrame = pandas.read_csv(old_results_path)
        df = df.loc[
            ~df['experiment_finished']
            | ~(df['synthesized_image_exec'].fillna(False))
        ]
        df = df.drop(columns=[
            'experiment_finished',
            'experiment_exception',
            'output',
            'base_image_exec',
            'configured_image_exec',
            'synthesized_image_exec',
            'jaccard_coefficient',
        ])
    else:
        # Load test set.
        test_set_path = dataset_data_dir / 'test_set.csv'

        if not test_set_path.is_file():
            raise FileNotFoundError(
                f'Test dataset does not exist at `{test_set_path}`. '
                f'Did you run dataset preparation?'
            )

        logger.verbose('Loading test set.')
        df: DataFrame = pandas.read_csv(test_set_path)
        df['set'] = 'test'

        # Load training set.
        if include_training_set:
            training_set_path = dataset_data_dir / 'training_set.csv'

            if not training_set_path.is_file():
                raise FileNotFoundError(
                    f'Training dataset does not exist at `{test_set_path}`. '
                    f'Did you run dataset preparation?'
                )

            logger.verbose('Loading training set.')
            training_set: DataFrame = pandas.read_csv(training_set_path)
            training_set['set'] = 'training'
            df = pandas.concat([df, training_set])

    # Load the filter if it exists.
    filter_path = dataset_data_dir / 'filter.csv'
    if filter_path.exists():
        df_filter: DataFrame = pandas.read_csv(filter_path)
    else:
        df_filter: DataFrame = DataFrame([], columns=df.columns)

    # Remove the filtered rows.
    filtered_rows = df.merge(df_filter, how='inner')
    df = (
        df.merge(
            filtered_rows,
            indicator=True,
            how='left',
        )
        .query('_merge=="left_only"')
        .drop(columns=['_merge'])
    )

    # Cast mounts to a list of Docker mounts.
    df['mounts'] = df['mounts'].apply(lambda row_mounts: [
        Mount(
            type=mount['Type'],
            source=mount['Source'],
            target=mount['Target'],
            read_only=mount['ReadOnly'],
        )
        for mount in ast.literal_eval(row_mounts)
    ])

    # Sort for easy reference.
    df = df.sort_values(['repo_name', 'branch', 'path'], ignore_index=True)

    # Set up results.
    experiment_data_dir.mkdir(parents=True)
    results = [
        [
            *row,
            False,
            'Filtered.',
            None,
            None,
            None,
            None,
            None,
            None,
        ]
        for _, row in filtered_rows.iterrows()
    ]
    results_data_frame = DataFrame(
        results,
        columns=[
            *df.columns,
            'experiment_finished',
            'experiment_exception',
            'output',
            'base_image_exec',
            'configured_image_exec',
            'synthesized_image_exec',
            'jaccard_coefficient',
            'synthesis_time',
        ],
    )
    results_data_frame.to_csv(experiment_data_dir / 'results.csv', index=False)

    # Run experiments.
    logger.verbose('Running synthesis experiments.')
    for _, row in df.iterrows():
        repo_dir_path = dataset_data_dir / row['repo_dir']
        dockerfile = repo_dir_path / row['path']
        context = repo_dir_path / row['context_path']
        mounts = row['mounts']
        for mount in mounts:
            src_path = Path(mount['Source'])
            if not src_path.is_absolute():
                mount['Source'] = str(repo_dir_path / src_path)
        sha = sha1(f'{row["repo_name"]}/{row["path"]}'.encode())  # noqa: S303
        tag = f'synth-experiment/{sha.hexdigest()}'
        output = experiment_data_dir / sha.hexdigest()
        try:
            experiment_result = run_docker_synthesis_experiment(
                dockerfile=dockerfile,
                context=context,
                mounts=mounts,
                tag=tag,
                system=system,
                output=output,
            )
        except Exception as e:  # noqa: B902
            logger.exception('Exception encountered while running experiment.')
            results.append([
                *row,
                False,
                str(e),
                output,
                None,
                None,
                None,
                None,
                None,
            ])
        else:
            logger.verbose('Experiment finished successfully.')
            results.append([
                *row,
                True,
                None,
                output,
                experiment_result.base_image_exec,
                experiment_result.configured_image_exec,
                experiment_result.synthesized_image_exec,
                experiment_result.jaccard_coefficient,
                experiment_result.synthesis_time,
            ])

        # Overwrite results.
        results_data_frame = DataFrame(
            results,
            columns=[
                *df.columns,
                'experiment_finished',
                'experiment_exception',
                'output',
                'base_image_exec',
                'configured_image_exec',
                'synthesized_image_exec',
                'jaccard_coefficient',
                'synthesis_time',
            ],
        )
        results_data_frame.to_csv(
            experiment_data_dir / 'results.csv',
            index=False,
        )

    # Compute results.
    return results_data_frame


def synthesize_debian_dockerfiles(dataset: str = DEFAULT_DATASET,
                                  rerun_failed: Optional[Path] = None,
                                  ) -> DataFrame:
    """Run the Debian Dockerfiles experiment.

    Parameters
    ----------
    dataset : str
        Name of the dataset to use for the experiment.
    rerun_failed : Optional[Path]
        Path to a previous debian dockerfiles synthesis experiment run. If
        provided, results from the previous execution will be loaded and any
        images where synthesis did not finish or was not successful will be
        rerun.

    Raises
    ------
    ValueError
        Raised if the provided dataset does not exist.
    FileNotFoundError
        Raised if the test set does not exist or ``include_training_set=True``
        and the training set does not exist.

    Returns
    -------
    DataFrame
        DataFrame containing experiment results.
    """
    return synthesize_configuration_scripts(
        dataset=dataset,
        system=ConfigurationSystem.DOCKER,
        include_training_set=False,
        experiment_data_dir=(
            EXPERIMENTS_OUTPUT_DIR
            / 'debian_dockerfiles'
            / f'debian_dockerfiles_{time.strftime("%Y-%m-%dT%H:%M:%S")}'
        ),
        rerun_failed=rerun_failed,
    )


def synthesize_debian_playbooks(dataset: str = DEFAULT_DATASET,
                                rerun_failed: Optional[Path] = None,
                                ) -> DataFrame:
    """Run the Debian playbooks experiment.

    Parameters
    ----------
    dataset : str
        Name of the dataset to use for the experiment.
    rerun_failed : Optional[Path]
        Path to a previous debian dockerfiles synthesis experiment run. If
        provided, results from the previous execution will be loaded and any
        images where synthesis did not finish or was not successful will be
        rerun.

    Raises
    ------
    ValueError
        Raised if the provided dataset does not exist.
    FileNotFoundError
        Raised if the test set does not exist or ``include_training_set=True``
        and the training set does not exist.

    Returns
    -------
    DataFrame
        DataFrame containing experiment results.
    """
    return synthesize_configuration_scripts(
        dataset=dataset,
        system=ConfigurationSystem.ANSIBLE,
        include_training_set=False,
        experiment_data_dir=(
            EXPERIMENTS_OUTPUT_DIR
            / 'debian_playbooks'
            / f'debian_playbooks_{time.strftime("%Y-%m-%dT%H:%M:%S")}'
        ),
        rerun_failed=rerun_failed,
    )
