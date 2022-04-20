"""Synth datasets preparation CLI."""


# Imports.
from argparse import Namespace

from synth.cli.typing import SubparsersAction
from synth.datasets import (
    DOCKER_DEBIAN_UBUNTU_DOCKERFILES_DATA_DIR,
    DOCKER_DEBIAN_UBUNTU_DOCKERFILES_METADATA,
    DOCKER_SIMPLE_DEBIAN_DOCKERFILES_DATA_DIR,
    DOCKER_SIMPLE_DEBIAN_DOCKERFILES_METADATA,
)
from synth.datasets.prepare import (
    prepare_curated_analysis_scripts,
    prepare_curated_dockerfiles,
    prepare_dockerfiles,
)


def init_subparser(subparsers: SubparsersAction):
    """Initialize the subcommand parser.

    Parameters
    ----------
    subparsers : SubparsersAction
        A subparsers action from the parent parser. The init function will
        use this action to initialize a parser for the subcommand.
    """
    parser = subparsers.add_parser(
        'prepare',
        help='Prepare Synth datasets for experiments.',
        description='Performs any pre-processing needed for Synth experiments.'
    )
    parser.set_defaults(run=run)


def run(args: Namespace):
    """Prepare synth datasets.

    Parameters
    ----------
    args : Namespace
        Command line arguments.
    """
    prepare_curated_analysis_scripts()
    # prepare_curated_dockerfiles()
    # prepare_dockerfiles(
    #     DOCKER_SIMPLE_DEBIAN_DOCKERFILES_METADATA,
    #     DOCKER_SIMPLE_DEBIAN_DOCKERFILES_DATA_DIR,
    # )
    prepare_dockerfiles(
        DOCKER_DEBIAN_UBUNTU_DOCKERFILES_METADATA,
        DOCKER_DEBIAN_UBUNTU_DOCKERFILES_DATA_DIR,
    )
