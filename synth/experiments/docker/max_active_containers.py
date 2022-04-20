"""An stress-test experiment for Docker max active containers.

This experiment continues to create active Docker containers until one fails,
to test Docker's capability to run containers based on different images.
"""


# Imports.
from time import time

from pandas import DataFrame

from synth.experiments.docker import (
    CMD, CONTAINER_LABEL, ExperimentDockerManager, IMAGES, NUM_TRIALS,
)
from synth.logging import logger


def run() -> tuple[DataFrame, list[tuple[str, int, Exception]]]:
    """Run the max active containers experiment.

    Returns
    -------
    DataFrame
        A dataframe containing experiment results.
    list[tuple[str, int, Exception]]
        (image, trial, exception) encountered during each trial.
    """
    logger.info('Starting max_active_containers experiment.')
    with ExperimentDockerManager() as manager:
        logger.info('Running trials.')
        results = []
        exceptions = []
        for trial in range(NUM_TRIALS):
            for name, tag in IMAGES:
                manager.recover_docker(images=[(name, tag)])
                image = f'{name}:{tag}'
                logger.info(f'Running trial `{trial}` for image `{image}`.')
                container_number = 1
                try:
                    while True:
                        logger.verbose(
                            f'Running container: `{container_number}`.'
                        )
                        start = time()
                        manager.client.containers.run(
                            image,
                            command=CMD,
                            detach=True,
                            labels=[CONTAINER_LABEL],
                        )
                        stop = time()
                        results.append(
                            (image, trial, container_number, stop - start),
                        )
                        container_number += 1
                except Exception as e:  # noqa: B902
                    logger.info(
                        f'Docker encountered an exception starting container '
                        f'`{container_number}`.',
                    )
                    logger.exception(e)
                    exceptions.append((image, trial, e))

    df = DataFrame(
        results,
        columns=[
            'image', 'trial', 'container_number', 'start_time',
        ],
    )
    return df, exceptions
