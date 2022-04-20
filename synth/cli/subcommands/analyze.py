"""Analyze subcommand."""


# Imports.
from argparse import Namespace
from pathlib import Path

import pandas
from pandas import DataFrame

from synth.cli.typing import SubparsersAction
from synth.datasets import ALL_DATASET_METADATA
from synth.logging import logger
from synth.paths import ANALYSIS_DIR, DATASET_DIR, DATASET_METADATA_DIR
from synth.synthesis.docker import analyze_and_record, process_analysis_script


def init_subparser(subparsers: SubparsersAction):
    """Initialize the subcommand parser.

    Parameters
    ----------
    subparsers : SubparsersAction
        A subparsers action from the parent parser. The init function will
        use this action to initialize a parser for the subcommand.
    """
    parser = subparsers.add_parser(
        'analyze',
        help='Analyze a configuration script and record the results.',
        description='Performs analysis of the configuration tasks found '
                    'within a configuration script, then records the '
                    'execution results in the knowledge base.',
    )
    parser.add_argument(
        '--analysis-script',
        help='If set, Synth will interpret the configuration_script argument '
             'as a custom Synth analysis script instead of trying to parse it '
             'as a regular configuration script. If specified with --dataset, '
             'Synth will treat the dataset as a collection of analysis '
             'scripts.',
        action='store_true',
    )
    parser.add_argument(
        '--dataset',
        help='If set, Synth will interpret the configuration_script argument '
             'as an existing dataset and will load and analyze all '
             'configuration scripts in the dataset. Available datasets are '
             f'{sorted(ALL_DATASET_METADATA.keys())}',
        action='store_true',
    )
    parser.add_argument(
        '--parse-only',
        help='If set, Synth will parse the configuration script but will not '
             'execute any of the tasks. Task execution will not be recorded.',
        action='store_true',
    )
    parser.add_argument(
        '--clean',
        help='If set with --dataset, Synth will re-analyze all configuration '
             'scripts in the dataset. By default analysis resumes at the last '
             'analyzed script in the dataset.',
        action='store_true',
    )
    parser.add_argument(
        'configuration_script',
        help='The path to a valid configuration script.',
    )
    parser.set_defaults(run=run)


def run(args: Namespace):
    """Analyze a configuration script.

    Parameters
    ----------
    args : Namespace
        Command line arguments.
    """
    # Get configuration script paths.
    if args.dataset:
        if args.configuration_script not in ALL_DATASET_METADATA:
            raise ValueError(
                f'Unrecognized dataset `{args.configuration_script}`.'
            )

        logger.info(f'Analyzing dataset `{args.configuration_script}`.')
        dataset_metadata_path = ALL_DATASET_METADATA[args.configuration_script]
        dataset_path = (
            DATASET_DIR
            / dataset_metadata_path.parent.relative_to(DATASET_METADATA_DIR)
            / dataset_metadata_path.stem
        )

        training_set_path = dataset_path / 'training_set.csv'
        if training_set_path.is_file():
            logger.info('Using training set index.')
            df: DataFrame = pandas.read_csv(training_set_path)
        else:
            logger.info('Using full dataset index.')
            df: DataFrame = pandas.read_csv(dataset_path / 'index.csv')

        # If analysis metadata for the dataset exists and we aren't cleaning,
        # load the metadata and remove all previously analyzed configuration
        # scripts from the set to be analyzed. Otherwise, remove the metadata.
        analysis_metadata_path = (
            ANALYSIS_DIR / f'{args.configuration_script}.csv'
        )
        if analysis_metadata_path.is_file() and not args.clean:
            logger.verbose('Resuming from previous analysis run.')
            analysis_metadata: DataFrame = pandas.read_csv(
                analysis_metadata_path,
            )
            df = (
                df.merge(
                    analysis_metadata,
                    indicator=True,
                    how='left',
                )
                .query('_merge=="left_only"')
                .drop(columns=['_merge'])
            )
        else:
            analysis_metadata_path.unlink(missing_ok=True)
            analysis_metadata: DataFrame = DataFrame(columns=[
                *df.columns,
                'success',
                'failed_at_task',
                'configuration_task_error',
            ])

        # Analyze all configuration scripts.
        try:
            for _, row in df.iterrows():
                script_path = dataset_path / row['repo_dir'] / row['path']
                context_path = (
                    dataset_path
                    / row['repo_dir']
                    / row.get('context_path', '.')
                )
                if 'setup_path' in row and not row.isna()['setup_path']:
                    setup_path = (
                        dataset_path
                        / row['repo_dir']
                        / row['setup_path']
                    )
                else:
                    setup_path = None

                if args.analysis_script:
                    analysis_result = process_analysis_script(script_path)
                else:
                    analysis_result = analyze_and_record(
                        path=script_path,
                        context=context_path,
                        setup_path=setup_path,
                        parse_only=args.parse_only,
                    )
                analysis_metadata = analysis_metadata.append(
                    {
                        **row,
                        'success': analysis_result.success,
                        'failed_at_task': analysis_result.failed_at_task,
                        'configuration_task_error':
                            analysis_result.configuration_task_error,
                    },
                    ignore_index=True,
                )
        finally:
            ANALYSIS_DIR.mkdir(exist_ok=True, parents=True)
            analysis_metadata.to_csv(analysis_metadata_path, index=False)
    elif args.analysis_script:
        script_path = Path(args.configuration_script).absolute()
        process_analysis_script(script_path)
    else:
        script_path = Path(args.configuration_script).absolute()
        context_path = script_path.parent
        analyze_and_record(
            path=script_path,
            context=context_path,
            parse_only=args.parse_only,
        )
