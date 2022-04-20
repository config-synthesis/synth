"""Synth datasets parse CLI."""


# Imports.
from argparse import Namespace
from pathlib import Path

from synth.cli.typing import SubparsersAction
from synth.datasets import ALL_DATASET_METADATA
from synth.datasets.parse_tasks import parse_tasks


def init_subparser(subparsers: SubparsersAction):
    """Initialize the subcommand parser.

    Parameters
    ----------
    subparsers : SubparsersAction
        A subparsers action from the parent parser. The init function will
        use this action to initialize a parser for the subcommand.
    """
    parser = subparsers.add_parser(
        'parse-tasks',
        help='Parse all tasks in a Synth dataset.',
        description='Parse all configuration tasks from a dataset and write a'
                    'csv file containing the executable, arguments, and '
                    'frequency of each task.'
    )
    parser.add_argument(
        'dataset',
        help='The dataset to parse.',
        choices=sorted(ALL_DATASET_METADATA.keys()),
    )
    parser.add_argument(
        '--use-index',
        action='store_true',
        help='Use the full dataset index instead of just the training set.',
    )
    parser.add_argument(
        '--output',
        '-o',
        type=Path,
        required=True,
        help='Output file path.',
    )
    parser.set_defaults(run=run)


def run(args: Namespace):
    """Download synth datasets.

    Parameters
    ----------
    args : Namespace
        Command line arguments.
    """
    df = parse_tasks(args.dataset, use_index=args.use_index)
    df.to_csv(args.output, index=False)
