"""Synth Docker experiments."""


# Imports.
from __future__ import annotations

import json
import os
from pathlib import Path
from time import sleep
from traceback import StackSummary
from typing import Any, Callable, Optional

import docker
import sh
from docker.errors import ImageNotFound
from sh import ErrorReturnCode

from synth.logging import logger
from synth.paths import DOCKER_CACHE_DIR


# Paths
DOCKER_DATA_DIR = Path('/var/lib/docker')
DOCKER_RUN_DIR = Path('/run/docker')

# Docker experiment constants.
IMAGES = [
    ('gcr.io/kaggle-gpu-images/python', 'v89'),
    ('gcr.io/kaggle-images/python', 'v89'),
    ('gcr.io/deeplearning-platform-release/base-cpu', 'm46'),
    ('alpine', '3.12.1'),
]
IMAGE_NAMES, IMAGE_TAGS = zip(*IMAGES)
CMD = "sh -c 'while true; do sleep 9999; done'"
CONTAINER_LABEL = 'synth:experiment:max_active_containers'
NUM_TRIALS = 30


def _check_docker_rw_access(cb: Callable[..., Any]) -> Callable[..., Any]:
    """Check for read access to the Docker data directory.

    If the current process does not have read access, a warning will be logged.

    Parameters
    ----------
    cb: Callable[..., Any]
        The callable to be wrapped.

    Returns
    -------
    Callable[..., Any]
        The wrapped callable.
    """
    def wrapper(*args, **kwargs) -> Any:
        """Check for Docker read/write access, then delegate.

        Returns
        -------
        Any
            The result of calling the original callable.
        """
        logger.verbose(f'Checking for read/write access to {DOCKER_DATA_DIR}.')
        if not os.access(DOCKER_DATA_DIR, os.R_OK | os.W_OK):
            logger.warning(
                f'Process user does not have read/write access to '
                f'{DOCKER_DATA_DIR}. This may cause errors. Try running '
                f'`sudo synth ...`.',
            )
        return cb(*args, **kwargs)
    return wrapper


class ExperimentDockerManager:
    """A Docker manager for running experiments.

    Instances of this class are context managers that automatically try to
    restore Docker to a pristine state an exit. This will overwrite whatever
    the Docker state was before entering the context, so use with caution.
    """

    def __init__(self):
        """Initialize a new manager."""
        self.client = docker.from_env()

    def __enter__(self) -> ExperimentDockerManager:
        """Enter the experiments Docker context.

        Returns
        -------
        ExperimentDockerManager
            self is returned for use within the context.
        """
        logger.verbose('Entering experiments Docker context.')
        return self

    def __exit__(self,
                 exc_type: Optional[type[BaseException]],
                 exc_val: Optional[BaseException],
                 exc_tb: Optional[StackSummary]):
        """Exit the experiments Docker context.

        Exiting the context automatically performs recovery.

        Parameters
        ----------
        exc_type : Optional[type[BaseException]]
            Exception type, if one occurred.
        exc_val : Optional[BaseException]
            Exception value, if one occurred.
        exc_tb : Optional[StackSummary]
            Exception traceback, if one occurred.
        """
        logger.verbose('Exiting experiments Docker context.')
        self.recover_docker()

    def stop_docker(self):
        """Stop Docker and Containerd."""
        systemctl_stop = ['systemctl', 'stop', 'docker', 'containerd']
        logger.verbose(f'Running `{" ".join(systemctl_stop)}` as root.')
        sh.sudo(*systemctl_stop)

    def start_docker(self):
        """Start Docker and Containerd."""
        systemctl_start = ['systemctl', 'start', 'docker', 'containerd']
        logger.verbose(f'Running `{" ".join(systemctl_start)}` as root.')
        sh.sudo(*systemctl_start)

    def kill(self, command: str):
        """Kill all instances of a command by name.

        This sends ``SIGKILL`` to stop processes immediately.

        Parameters
        ----------
        command : str
            Process command name.
        """
        try:
            killall = ['killall', '--signal=SIGKILL', command]
            logger.verbose(f'Running `{" ".join(killall)}` as root.')
            sh.sudo(*killall)
        except ErrorReturnCode as e:
            msg = str(e.stderr, encoding='utf-8').strip()
            if not msg.endswith('no process found'):
                raise

    def kill_containerd_shims(self):
        """Kill all Containerd shims.

        This sends ``SIGKILL`` to all ``containerd-shim`` processes to stop
        them immediately.
        """
        self.kill('containerd-shim')

    def kill_experiment_commands(self):
        """Kill all Containerd shims.

        This sends ``SIGKILL`` to all experiment command processes to stop
        them immediately.
        """
        self.kill(CMD.split(maxsplit=1)[0])

    def reinit_docker_mounts(self):
        """Redo all Docker mounts.

        Recursively unmount the Docker data and run directories, then make a
        new filesystem on the underlying devices and mount them. This has the
        effect of quickly wiping both directories.
        """
        umount_data = ['umount', '-f', '--recursive', str(DOCKER_DATA_DIR)]
        logger.verbose(f'Running `{" ".join(umount_data)}` as root.')
        try:
            sh.sudo(*umount_data)
        except ErrorReturnCode as e:
            msg = str(e.stderr, encoding='utf-8').strip()
            if not msg.endswith('not mounted'):
                raise

        umount_run = ['umount', '-f', '--recursive', str(DOCKER_RUN_DIR)]
        logger.verbose(f'Running `{" ".join(umount_run)}` as root.')
        try:
            sh.sudo(*umount_run)
        except ErrorReturnCode as e:
            msg = str(e.stderr, encoding='utf-8').strip()
            if not msg.endswith('not mounted'):
                raise

        # Let the system finish whatever with the devices so we can mkfs.
        sleep_time = 30
        logger.verbose(f'Sleeping for {sleep_time}s')
        sleep(sleep_time)

        mkfs_data = ['mkfs', '-F', '-t', 'ext4', '/dev/docker/docker-data']
        logger.verbose(f'Running `{" ".join(mkfs_data)}` as root.')
        sh.sudo(*mkfs_data)

        mkfs_run = ['mkfs', '-F', '-t', 'ext4', '/dev/docker/docker-run']
        logger.verbose(f'Running `{" ".join(mkfs_run)}` as root.')
        sh.sudo(*mkfs_run)

        mount_data = ['mount', '/dev/docker/docker-data', str(DOCKER_DATA_DIR)]
        logger.verbose(f'Running `{" ".join(mount_data)}` as root.')
        sh.sudo(*mount_data)

        mount_run = ['mount', '/dev/docker/docker-run', str(DOCKER_RUN_DIR)]
        logger.verbose(f'Running `{" ".join(mount_run)}` as root.')
        sh.sudo(*mount_run)

    def recover_docker(self, images: Optional[list[tuple[str, str]]] = None):
        """Restore Docker to a pristine running condition.

        This stops Docker and Containerd if they are running, kills all
        Containerd shims, restores the Docker data and run directories, and
        then restarts Docker and Containerd.

        Parameters
        ----------
        images : list[tuple[str, str]]
            Optional list of images to pull during recovery. Uses default
            if not provided.
        """
        logger.info('Attempting to recover Docker.')
        self.stop_docker()
        self.kill_containerd_shims()
        self.kill_experiment_commands()
        self.reinit_docker_mounts()
        with logger.indent():
            sleep_time = 30
            logger.verbose(f'Sleeping for {sleep_time}s')
            sleep(sleep_time)
        self.start_docker()
        if images:
            self.pull_images(images)
        else:
            self.pull_images()

    def pull_images(self, images: list[tuple[str, str]] = IMAGES):
        """Pull any missing Docker images used for experiments.

        Parameters
        ----------
        images : list[tuple[str, str]]
            Optional list of images to pull. Defaults to all experiment images.
        """
        # Create the Docker cache dir if it doesn't already exist.
        DOCKER_CACHE_DIR.mkdir(exist_ok=True, parents=True)

        # Search for missing images.
        missing_images = set()
        for name, tag in images:
            try:
                self.client.images.get(f'{name}:{tag}')
            except ImageNotFound:
                missing_images.add((name, tag))

        # Pull images.
        if missing_images:
            logger.info(
                'Loading Docker images for experiment. This may take a long '
                'time.',
            )
            for name, tag in missing_images:
                cache_file = f'{name}:{tag}.tar.gz'.replace('/', '_')
                cache_path = DOCKER_CACHE_DIR / cache_file
                if cache_path.exists():
                    logger.verbose(
                        f'Attempting to load `{name}:{tag}` from cache at '
                        f'`{cache_path}`.'
                    )
                    try:
                        sh.docker(
                            sh.gzip('-cd', str(cache_path), _piped=True),
                            'image',
                            'load'
                        )
                    except ErrorReturnCode as e:
                        logger.verbose(
                            f'Cache load failed with status `{e.exit_code}`. '
                            f'Will delete cache and pull image.\n'
                            f'stdout:\n{e.stdout}\n'
                            f'stderr:\n{e.stderr}\n'
                        )
                    else:
                        logger.verbose('Cache load success.')
                        continue

                cache_path.unlink(missing_ok=True)
                logger.verbose(f'Pulling `{name}:{tag}`.')
                for output in self.client.api.pull(name, tag, stream=True):
                    lines = str(output, encoding='utf-8').splitlines()
                    for msg in map(json.loads, lines):
                        logger.verbose(msg)

        # Cache images.
        for name, tag in images:
            cache_file = f'{name}:{tag}.tar.gz'.replace('/', '_')
            cache_path = DOCKER_CACHE_DIR / cache_file
            if cache_path.exists():
                continue

            try:
                logger.verbose(f'Caching `{name}:{tag}` at `{cache_path}`.')
                sh.gzip(
                    sh.docker('image', 'save', f'{name}:{tag}', _piped=True),
                    '-9r',
                    _out=str(cache_path),
                )
            except ErrorReturnCode:
                pass
