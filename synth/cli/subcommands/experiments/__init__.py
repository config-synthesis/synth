"""Synth experiments CLI."""


# Imports.
from synth.cli.subcommands.experiments import docker, synthesis
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
        'experiments',
        help='Run Synth experiments.',
    )
    subparsers = parser.add_subparsers(
        dest='subcommand',
        title='synth experiment subcommands',
        description='Run Synth experiments.',
        help='Run one of these commands to get started.',
        required=True,
    )
    docker.init_subparser(subparsers)
    synthesis.init_subparser(subparsers)
