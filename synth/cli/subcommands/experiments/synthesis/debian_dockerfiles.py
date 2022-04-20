"""Synth experiments CLI for synthesizing Debian Dockerfiles."""


# Imports.
from argparse import Namespace
from pathlib import Path

import pandas

from synth.cli.typing import SubparsersAction
from synth.experiments.synthesis.debian import (
    AVAILABLE_DATASETS,
    DEFAULT_DATASET,
    synthesize_debian_dockerfiles
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
        'debian-dockerfiles',
        help='Synthesize Debian Dockerfiles.',
        description='Synthesize Debian Dockerfiles.',
    )
    parser.add_argument(
        '--rerun-failed',
        type=Path,
        required=False,
        help='Path to a previous debian dockerfiles synthesis experiment run. '
             'If provided, results from the previous execution will be loaded '
             'and any images where synthesis did not finish or was not '
             'successful will be rerun.',
    )
    parser.add_argument(
        'dataset',
        help=f'The dataset to use in the experiment. '
             f'Defaults to {DEFAULT_DATASET}',
        choices=AVAILABLE_DATASETS,
        nargs='?',
        default=DEFAULT_DATASET,
    )
    parser.set_defaults(run=run)


def run(args: Namespace):
    """Synthesize Debian Dockerfiles.

    Parameters
    ----------
    args : Namespace
        Command line arguments.
    """
    results = synthesize_debian_dockerfiles(
        args.dataset,
        rerun_failed=args.rerun_failed,
    )
    with pandas.option_context('display.max_columns', 0,
                               'display.max_colwidth', 0,
                               'display.max_rows', 0):
        print(results)
