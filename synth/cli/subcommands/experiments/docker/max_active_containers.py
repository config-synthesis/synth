"""Synth max active containers experiment CLI."""


# Imports.
from argparse import Namespace

import pandas

from synth.cli.typing import SubparsersAction
from synth.cli.util import prompt_no_vagrant
from synth.experiments.docker import max_active_containers
from synth.paths import EXPERIMENTS_OUTPUT_DIR


EXPERIMENT_OUTPUT_DIR = EXPERIMENTS_OUTPUT_DIR / 'max_active_containers'


def init_subparser(subparsers: SubparsersAction):
    """Initialize the subcommand parser.

    Parameters
    ----------
    subparsers : SubparsersAction
        A subparsers action from the parent parser. The init function will
        use this action to initialize a parser for the subcommand.
    """
    parser = subparsers.add_parser(
        'max-active-containers',
        help='Run the Synth max active containers experiment.',
        description='Run the Synth max active containers experiment. This '
                    'experiment continues to start Docker containers for '
                    'a set of images until an error is encountered. It '
                    'records how many containers could be started based on '
                    'each image.',
    )
    parser.set_defaults(run=run)


@prompt_no_vagrant
def run(args: Namespace):
    """Run the Docker max active containers experiment..

    Parameters
    ----------
    args : Namespace
        Command line arguments.
    """
    # Create output directories.
    output_dir = EXPERIMENT_OUTPUT_DIR / args.time_str
    output_dir.mkdir(parents=True, exist_ok=True)
    exceptions_dir = output_dir / 'exceptions'
    exceptions_dir.mkdir(parents=True, exist_ok=True)

    # Run experiment.
    df, exceptions = max_active_containers.run()

    # Write output files.
    df.to_csv(output_dir / 'results.csv', index=False)
    for image, trial, exception in exceptions:
        image_dir = exceptions_dir / image
        image_dir.mkdir(parents=True, exist_ok=True)
        with open(image_dir / str(trial), 'w') as fd:
            fd.write(str(exception))

    with pandas.option_context('display.max_rows', None,
                               'display.max_columns', None):
        # Print container time stats.
        print('Container Start Times By Image')
        print(
            df[['image', 'start_time']]
            .groupby('image')
            .describe()
        )
        print()

        # Print max container stats.
        print('Max Containers Per Trial By Image')
        print(
            df[['image', 'trial', 'container_number']]
            .groupby(['image', 'trial'])
            .max()
            .rename(columns={'container_number': 'max_containers'})
            .groupby('image')
            .describe()
        )
