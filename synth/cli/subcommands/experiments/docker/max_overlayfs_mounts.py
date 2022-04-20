"""Synth max overlayfs experiment CLI."""


# Imports.
from argparse import Namespace

import pandas

from synth.cli.typing import SubparsersAction
from synth.cli.util import prompt_no_vagrant
from synth.experiments.docker import max_overlayfs_mounts
from synth.paths import EXPERIMENTS_OUTPUT_DIR


EXPERIMENT_OUTPUT_DIR = EXPERIMENTS_OUTPUT_DIR / 'max_overlayfs_mounts'


def init_subparser(subparsers: SubparsersAction):
    """Initialize the subcommand parser.

    Parameters
    ----------
    subparsers : SubparsersAction
        A subparsers action from the parent parser. The init function will
        use this action to initialize a parser for the subcommand.
    """
    parser = subparsers.add_parser(
        'max-overlayfs-mounts',
        help='Run the Synth max overlayfs mounts experiment.',
        description='Run the Synth max overlayfs mounts experiment. This '
                    'experiment continues to mount Docker images using the '
                    'overlay filesystem until an error is encountered. It '
                    'records how many mounts could be created based on each '
                    'image.'
    )
    parser.set_defaults(run=run)


@prompt_no_vagrant
def run(args: Namespace):
    """Run the Docker max overlayfs mounts experiment.

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
    df, exceptions = max_overlayfs_mounts.run()

    # Write output files.
    df.to_csv(output_dir / 'results.csv', index=False)
    for image, trial, exception in exceptions:
        image_dir = exceptions_dir / image
        image_dir.mkdir(parents=True, exist_ok=True)
        with open(image_dir / str(trial), 'w') as fd:
            fd.write(str(exception))

    if df.empty:
        print('No Results to Report')
        return

    with pandas.option_context('display.max_rows', None,
                               'display.max_columns', None):
        # Print container time stats.
        print('Mount Times By Image')
        print(
            df[['image', 'mount_time']]
            .groupby('image')
            .describe()
        )
        print()

        # Print max container stats.
        print('Max Mounts Per Trial By Image')
        print(
            df[['image', 'trial', 'mount_number']]
            .groupby(['image', 'trial'])
            .max()
            .rename(columns={'mount_number': 'max_mounts'})
            .groupby('image')
            .describe()
        )
