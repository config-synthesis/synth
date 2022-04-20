"""Synth configuration synthesis support."""


# Imports.
from collections.abc import Iterable
from itertools import chain
from typing import Optional

from docker.types import Mount

from synth.logging import logger
from synth.synthesis.classes import (
    ConfigurationChange,
    ConfigurationSystem,
    ConfigurationTask,
)
from synth.synthesis.classes import ConfigurationTaskError
from synth.synthesis.configuration_scripts import get_writer
from synth.synthesis.docker import diff_images, get_runner
from synth.synthesis.search import get_task_ordering, get_task_set


# Constants
CONFIGURATION_TASK_TIMEOUT = 3600


def synthesize_configuration_tasks(
        system: ConfigurationSystem,
        image: Optional[str] = None,
        changes: Optional[Iterable[ConfigurationChange]] = None,
        order: bool = True,
        base_image: Optional[str] = None,
        is_runner_image: Optional[bool] = None,
        mounts: Optional[list[Mount]] = None) -> list[ConfigurationTask]:
    """Synthesize a sequence of configuration tasks for a configuration script.

    Parameters
    ----------
    system : ConfigurationSystem
        The configuration system of the produced script.
    image : Optional[str]
        A Docker image to use for synthesis. The changes to this Docker image
        will be used as the initial change set to reproduce. ``base_image``
        must also be provided.
    changes : Optional[Iterable[ConfigurationChange]]
        Configuration changes that should be reproduced. This can be in
        addition to the changes diffed from ``image`` and ``base_image`` if
        they are provided. Otherwise, it is a manual change set.
    order : bool
        If resulting tasks should be ordered. This is true by default, but can
        be disabled due to the complexity of task ordering.
    base_image : Optional[str]
        The base Docker image. This will be used for diffing changes from
        ``image`` if both are provided. It will also be used as the base image
        for task ordering.
    is_runner_image : bool
        Whether the base image is a Synth runner image.
    mounts : Optional[list[Mount]]
        Docker mounts to use during search.

    Returns
    -------
    list[ConfigurationTask]
        A list of configuration tasks that makes up the configuration script.
    """
    if mounts is None:
        mounts = []

    # Initialize the changes set.
    if changes is not None:
        changes = set(changes)
    else:
        changes = set()

    # If an image and base image ar provided, get the changes from them.
    if image and base_image:
        logger.info(
            f'Loading changes by diffing the Docker images '
            f'{image} and {base_image}'
        )
        changes.update(diff_images(
            base_image,
            image,
            mounts=mounts,
        ))

    # Process tasks for each level.
    level = 1
    tasks = []
    previous_image = base_image
    while level >= 0:
        logger.info(f'Running the synthesis process for level=`{level}`.')

        # Get tasks.
        results = get_task_set(changes, system=system, level=level)

        # Log the task set.
        task_set_str = ['Search found the following configuration task set:']
        for result in results:
            task_set_str.append(f'    {result.original_task}')
            for k, v in result.mapping.source_arguments.items():
                task_set_str.append(f'        {k.value} => {v.value}')

        logger.verbose('\n'.join(task_set_str))

        # Order the tasks if requested.
        if order:
            results = get_task_ordering(
                results,
                image=previous_image,
                is_runner_image=is_runner_image,
                mounts=mounts,
            )
        else:
            logger.info(
                'Skipping task ordering step. Results will be unordered.'
            )

        # Update tasks.
        tasks += [result.task for result in results]

        # If images are provided, and we have ordered tasks, then run the
        # synthesized tasks against the base image and then take the diff.
        logger.info(
            f'Running the synthesized configuration script for '
            f'level={level}.'
        )
        if image and base_image and order:
            with get_runner(system=system,
                            image=base_image,
                            is_runner_image=is_runner_image,
                            mounts=mounts) as runner:
                # Run the script against a clean image.
                try:
                    for task in tasks:
                        runner.run_task(
                            task,
                            timeout=CONFIGURATION_TASK_TIMEOUT
                        )
                except ConfigurationTaskError:
                    logger.exception(
                        'Encountered an exception while rebuilding the '
                        'synthesized image from the synthesized configuration '
                        'script. This should not happen and is likely a bug.'
                    )
                    raise

                # Commit the container and take the diff.
                synthesized_tag = (
                    f'synth-synthesized/{runner.container.short_id}'
                )
                labels = {
                    'synth': 'true',
                    'synth.task': 'configuration script synthesis',
                }
                logger.verbose(
                    f'Committing synthesized image with tag '
                    f'`{synthesized_tag}`.'
                )
                runner.container.commit(
                    repository=synthesized_tag,
                    conf={'Labels': labels},
                )
                new_changes = diff_images(
                    base_image,
                    synthesized_tag,
                    mounts=mounts,
                )

                if previous_image != base_image:
                    runner.client.images.remove(previous_image)

                previous_image = synthesized_tag
        else:
            new_changes = set(chain.from_iterable(
                result.task.changes for result in results
            ))

        changes = changes - new_changes
        level -= 1

        if not changes:
            logger.verbose('No more changes to reproduce.')
            break

    if previous_image != base_image:
        runner.client.images.remove(previous_image)

    return tasks


def synthesize_configuration_script(
        system: ConfigurationSystem,
        image: Optional[str] = None,
        changes: Optional[Iterable[ConfigurationChange]] = None,
        order: bool = True,
        base_image: Optional[str] = None,
        is_runner_image: Optional[bool] = None,
        mounts: Optional[list[Mount]] = None) -> str:
    """Synthesize a configuration script.

    Parameters
    ----------
    system : ConfigurationSystem
        The configuration system of the produced script.
    image : Optional[str]
        A Docker image to use for synthesis.
    changes : Iterable[ConfigurationChange]
        Configuration changes that should be reproduced.
    order : bool
        If resulting tasks should be ordered. This is true by default, but can
        be disabled due to the complexity of task ordering.
    base_image : str
        The base Docker image. If provided this image will be used for
        ordering.
    is_runner_image : bool
        Whether the base image is a Synth runner image.
    mounts : Optional[list[Mount]]
        Docker mounts to use during ordering.

    Returns
    -------
    str
        A configuration script that reproduces the desired changes.
    """
    # Get tasks.
    tasks = synthesize_configuration_tasks(
        system=system,
        image=image,
        changes=changes,
        order=order,
        base_image=base_image,
        is_runner_image=is_runner_image,
        mounts=mounts,
    )

    # Print the final synthesis result.
    logger.info('Done. Writing configuration script.')
    writer = get_writer(system)
    return writer(tasks)
