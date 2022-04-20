"""Synth Docker experiments CLI for Docker recovery."""


# Imports.
from argparse import Namespace

from synth.cli.typing import SubparsersAction
from synth.cli.util import prompt_no_vagrant
from synth.experiments.docker import ExperimentDockerManager


def init_subparser(subparsers: SubparsersAction):
    """Initialize the subcommand parser.

    Parameters
    ----------
    subparsers : SubparsersAction
        A subparsers action from the parent parser. The init function will
        use this action to initialize a parser for the subcommand.
    """
    parser = subparsers.add_parser(
        'recover-docker',
        help='Perform Docker recovery.',
        description='Restore Docker to a usable state for experiments.',
    )
    parser.set_defaults(run=run)


@prompt_no_vagrant
def run(args: Namespace):
    """Recover Docker.

    Parameters
    ----------
    args : Namespace
        Command line arguments.
    """
    ExperimentDockerManager().recover_docker()
