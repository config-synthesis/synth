"""Synth datasets unpack CLI."""


# Imports.
from argparse import Namespace
from pathlib import Path

from synth.cli.typing import SubparsersAction
from synth.datasets.download import unpack


def init_subparser(subparsers: SubparsersAction):
    """Initialize the subcommand parser.

    Parameters
    ----------
    subparsers : SubparsersAction
        A subparsers action from the parent parser. The init function will
        use this action to initialize a parser for the subcommand.
    """
    parser = subparsers.add_parser(
        'unpack',
        help='Unpack Synth datasets from an archive.',
        description='Unpack archived Synth datasets and place them in the '
                    'directory that Synth will use for experiments.'
    )
    parser.add_argument(
        'archive',
        type=Path,
        help='Archive file path.',
    )
    parser.set_defaults(run=run)


def run(args: Namespace):
    """Unpack synth datasets.

    Parameters
    ----------
    args : Namespace
        Command line arguments.
    """
    unpack(args.archive)
