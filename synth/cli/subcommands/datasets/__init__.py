"""Synth datasets CLI."""

# Imports.
from synth.cli.subcommands.datasets import (
    download, parse_tasks, prepare, unpack,
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
        'datasets',
        help='Work with Synth datasets.',
    )
    subparsers = parser.add_subparsers(
        dest='subcommand',
        title='synth datasets subcommands',
        description='Work with Synth datasets.',
        help='Run one of these commands to get started.',
        required=True,
    )
    download.init_subparser(subparsers)
    parse_tasks.init_subparser(subparsers)
    prepare.init_subparser(subparsers)
    unpack.init_subparser(subparsers)
