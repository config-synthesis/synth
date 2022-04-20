"""Synth Docker experiments CLI."""


# Imports.
from synth.cli.subcommands.experiments.docker import (
    max_active_containers,
    max_created_containers,
    max_overlayfs_mounts,
    pull_images,
    recover_docker,
)
from synth.cli.typing import SubparsersAction


def init_subparser(subparsers: SubparsersAction):
    """Initialize the subcommand parser.

    Parameters
    ----------
    subparsers : SubparsersAction
        A subparsers action from the parent parser. The init function will
        use this action to initialize a parser for the subcommand.
    """
    parser = subparsers.add_parser(
        'docker',
        help='Run Synth Docker experiments.',
    )
    subparsers = parser.add_subparsers(
        dest='subcommand',
        title='synth docker experiment subcommands',
        description='Run Synth Docker experiments.',
        help='Run one of these commands to get started.',
        required=True,
    )
    max_active_containers.init_subparser(subparsers)
    max_created_containers.init_subparser(subparsers)
    max_overlayfs_mounts.init_subparser(subparsers)
    pull_images.init_subparser(subparsers)
    recover_docker.init_subparser(subparsers)
