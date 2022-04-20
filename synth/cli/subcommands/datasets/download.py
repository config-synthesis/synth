"""Synth datasets download CLI."""


# Imports.
from argparse import Namespace

from synth.cli.typing import SubparsersAction
from synth.datasets.download import download_all


def init_subparser(subparsers: SubparsersAction):
    """Initialize the subcommand parser.

    Parameters
    ----------
    subparsers : SubparsersAction
        A subparsers action from the parent parser. The init function will
        use this action to initialize a parser for the subcommand.
    """
    parser = subparsers.add_parser(
        'download',
        help='Download Synth datasets.',
        description='Download code from pre-existing Synth datasets.'
    )
    parser.add_argument(
        '--clean',
        help='If specified, existing repos will be re-cloned. Otherwise any '
             'existing repos will be kept as is and only new repos will be '
             'cloned.',
        action='store_true',
    )
    parser.add_argument(
        '--n',
        type=int,
        help='Server number (1..of). If specified, this number will be used '
             'to clone only repos for this specific server based on a '
             'round-robin approach.',
    )
    parser.add_argument(
        '--of',
        type=int,
        help='Total number of servers. Must be specified if ``n`` is '
             'provided.',
    )
    parser.set_defaults(run=run)


def run(args: Namespace):
    """Download synth datasets.

    Parameters
    ----------
    args : Namespace
        Command line arguments.
    """
    download_all(clean=args.clean, n=args.n, of=args.of)
