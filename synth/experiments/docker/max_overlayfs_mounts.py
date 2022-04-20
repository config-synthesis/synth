"""A stress-test experiment for overlayfs mounts of Docker images.

This experiment continues to mount Docker images using overlayfs until one
fails to test differences between images.
"""


# Imports.
import ctypes
import gc
import shutil
import subprocess
from contextlib import contextmanager
from functools import lru_cache
from itertools import chain
from multiprocessing import Process
from multiprocessing.connection import Connection, Pipe
from pathlib import Path
from time import sleep, time
from typing import Generator, Union

from docker import DockerClient
from pandas import DataFrame

from synth.exceptions import MountException
from synth.experiments.docker import (
    _check_docker_rw_access, DOCKER_DATA_DIR, ExperimentDockerManager, IMAGES,
    NUM_TRIALS,
)
from synth.logging import logger


# Paths
DOCKER_OVERLAY2_LINK_DIR = DOCKER_DATA_DIR / 'overlay2/l'
MOUNT_TMP_DIR = Path('/tmp/max-overlayfs-mounts')  # noqa: S108

# Other constants.
MB_500 = 500 * 2**20


def _reset_overlayfs_mounts():
    """Unmount all overlayfs mounts and clean up the mount temp dir."""
    # There's the potential that adding swap could help resolve memory issues
    # when removing mounts after a single trial. If that becomes necessary
    # later, try the following.
    #
    # sudo fallocate -l 1G /swapfile
    # sudo chmod 600 /swapfile
    # sudo mkswap /swapfile
    # sudo swapon /swapfile
    # sudo swapon --show
    # sudo echo '60' > /proc/sys/vm/swappiness
    # sudo umount --all --types=overlay
    # sudo echo '0' > /proc/sys/vm/swappiness
    # sudo swapoff /swapfile
    # sudo rm /swapfile

    umount = ['sudo', 'umount', '--all', '--types=overlay']
    logger.verbose(f'Running `{" ".join(umount)}`.')
    subprocess.run(umount, capture_output=True, check=True)

    MOUNT_TMP_DIR.mkdir(exist_ok=True, parents=True)
    for image_dir in MOUNT_TMP_DIR.glob('*/'):
        shutil.rmtree(image_dir)


@lru_cache
def _get_lower_dirs(client: DockerClient, image: str) -> str:
    """Get the lower dirs for mounting an image.

    Parameters
    ----------
    client : DockerClient
        Configured Docker client.
    image : str
        Name of the image to get lower dirs for.

    Returns
    -------
    A string formatted for use with the ``lowerdir`` argument in overlayfs
    mounts. Includes the image's diff directory, plus all of the image's
    lower directories.
    """
    inspect = client.api.inspect_image(image)

    data = inspect['GraphDriver']['Data']
    upper_dir = [data['UpperDir']]
    if 'LowerDir' in data:
        lower_dir = data['LowerDir'].split(':')
    else:
        lower_dir = []

    return ':'.join(map(
        lambda d: (Path(d).parent / 'link').read_text(),
        chain(upper_dir, lower_dir),
    ))


@contextmanager
def _reserve_memory(num_bytes: int = MB_500) -> Generator[None, None, None]:
    """Reserve system memory.

    This function allocates some amount of memory to the current process, then
    yields. It then deletes the memory, garbage collects, and waits 20 seconds
    to let the system reclaim memory. This can be used as a buffer when other
    processes are expected to consume all or most of the system memory.

    Parameters
    ----------
    num_bytes : int
        Number of bytes to reserve. Defaults to 500 MB.

    Returns
    -------
    Generator[None, None, None]
        Yields before releasing memory.
    """
    memory_type = ctypes.c_char * num_bytes
    mem = memory_type()
    try:
        yield
    finally:
        logger.verbose(
            'Releasing reserved memory, garbage collecting, and sleeping.'
        )
        del mem
        gc.collect()
        sleep(20)


def _mount_image(image: str, mount_number: int, lower_dirs: str):
    """Mount an image.

    Overlayfs directories will be created under
    `MOUNT_TMP_DIR / image / mount_number`.

    Parameters
    ----------
    image : str
        Docker image identifier.
    mount_number : int
        Unique mount number.
    lower_dirs : str
        Lower directories used as the `lowerdir` argument when creating the
        overlayfs mount.
    """
    mount_dir: Path = (
        MOUNT_TMP_DIR / image.replace('/', '_') / str(mount_number)
    )
    mount_dir.mkdir(exist_ok=True, parents=True)

    upper_dir: Path = mount_dir / 'diff'
    upper_dir.mkdir(exist_ok=True, parents=True)

    work_dir: Path = mount_dir / 'work'
    work_dir.mkdir(exist_ok=True, parents=True)

    merged_dir: Path = mount_dir / 'merged'
    merged_dir.mkdir(exist_ok=True, parents=True)

    name = f'{image}/{mount_number}'
    options = f'lowerdir={lower_dirs},upperdir={upper_dir},workdir={work_dir}'
    mount = [
        'sudo', 'mount', '-t', 'overlay', name, '-o', options, str(merged_dir),
    ]
    try:
        subprocess.run(
            mount,
            cwd=DOCKER_OVERLAY2_LINK_DIR,
            capture_output=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        raise MountException(
            msg=str(e.stderr, 'utf-8').strip(),
            fs='overlay',
            mnt=str(merged_dir),
            options=options,
        ) from e


def _mount_subprocess(image: str,
                      lower_dirs: str,
                      connection: Connection) -> None:
    """Mount subprocess main function.

    Continue to mount an image until an exception is encountered. Send all
    results to the main process via ``connection``.

    Parameters
    ----------
    image : str
        Docker image name.
    lower_dirs : str
        Overlay lower dirs string.
    connection : Connection
        Connection object to send results to.
    """
    with _reserve_memory():
        mount_number = 1
        try:
            while True:
                start = time()
                logger.verbose(f'Creating mount `{mount_number}`.')
                _mount_image(image, mount_number, lower_dirs)
                stop = time()
                connection.send(
                    (mount_number, stop - start)
                )
                mount_number += 1
        except Exception as e:  # noqa: B902
            logger.info(
                f'Encountered an exception while creating mount '
                f'`{mount_number}`.'
            )
            logger.exception(e)
            connection.send(e)


@_check_docker_rw_access
def run() -> tuple[DataFrame, list[tuple[str, int, Union[Exception, str]]]]:
    """Run the max overlayfs mounts experiment.

    Returns
    -------
    DataFrame
        A dataframe containing experiment results.
    list[tuple[str, int, Union[Exception, str]]]
        (image, trial, exception) encountered during each trial. Exception
        may either be an exception object or a string containing an exception
        message.
    """
    logger.info('Starting max_overlayfs_mounts experiment.')
    with ExperimentDockerManager() as manager:
        # Reset the system overlayfs mounts before running.
        logger.verbose('Resetting overlayfs mounts.')
        _reset_overlayfs_mounts()

        # Run trials.
        logger.verbose('Starting trials.')
        results = []
        exceptions = []
        for trial in range(NUM_TRIALS):
            for name, tag in IMAGES:
                manager.recover_docker(images=[(name, tag)])
                image = f'{name}:{tag}'
                logger.info(f'Running trial `{trial}` for `{image}`.')

                logger.verbose('Getting lower dirs for image.')
                lower_dirs = _get_lower_dirs(manager.client, image)

                # Create a mount subprocess to handle the actual mounting.
                logger.verbose('Starting mount process.')
                parent_connection, child_connection = Pipe()
                mount_proc = Process(
                    target=_mount_subprocess,
                    args=(image, lower_dirs, child_connection)
                )
                mount_proc.start()

                # Continue to poll the mount process until we break on some
                # condition. We want to keep reading results until we know we
                # have read everything.
                while True:

                    # Continue to poll for results with a 1 second timeout
                    # while the mount process is still alive. If the mount
                    # process dies we will read all data sent and then exit.
                    # If poll returns true, we will read all data sent and
                    # then continue to wait.
                    while mount_proc.is_alive() \
                            and not parent_connection.poll(1):  # noqa: N400
                        pass

                    # Record whether or not the mount process is alive before
                    # reading any results. If it is not alive before reading,
                    # then exit afterwards. Otherwise we'll continue to the
                    # next iteration of the loop to continue waiting. The
                    # mount process may die while reading results. If this
                    # happens we'll catch it on the next iteration of the loop.
                    is_alive = mount_proc.is_alive()

                    # Read all results.
                    while parent_connection.poll():
                        result = parent_connection.recv()
                        if isinstance(result, tuple):
                            mount_number, mount_time = result
                            results.append(
                                (image, trial, mount_number, mount_time)
                            )
                        elif isinstance(result, Exception):
                            exceptions.append((image, trial, result))
                        else:
                            raise Exception('Unrecognized result.')

                    # If the mount process was not alive before we began
                    # processing results, there cannot be any new results.
                    # Stop processing.
                    if not is_alive:
                        if mount_proc.exitcode:
                            result = f'Exited With: `{mount_proc.exitcode}`'
                            logger.verbose(result)
                            exceptions.append((image, trial, result))
                        break

                # Close the mount process.
                mount_proc.close()

                # Reset the overlayfs mounts for the next trial.
                logger.info('Resetting mounts.')
                _reset_overlayfs_mounts()

    df = DataFrame(
        results,
        columns=[
            'image', 'trial', 'mount_number', 'mount_time'
        ]
    )
    return df, exceptions
