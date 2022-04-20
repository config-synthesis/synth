"""Synth CLI."""


# Imports.
import sys
import time
from argparse import ArgumentParser
from pathlib import Path

from synth.cli.subcommands import (
    analyze, build_image, datasets, experiments, synthesize,
)
from synth.logging import (
    install_stream_handler, logger, LOGGING_DIR, set_level,
)
from synth.util.shell import join, quote


def _default_subcommand_run(*args, **kwargs):
    """Run default subcommand.

    Raises
    ------
    NotImplementedError
        This function raises a sane NotImplementedError. If a subcommand does
        not set the ``run`` property, this function will be called instead.
    """
    raise NotImplementedError(
        'The specified subcommand has not provided a run function. This is '
        'an error with Synth, not you.',
    )


def run():
    """Parse args and run."""
    # Create main parser.
    parser = ArgumentParser(
        prog='synth',
        description='Synthesize environment configurations.',
    )
    parser.add_argument(
        '--verbose',
        '-v',
        action='count',
        default=0,
        help='Verbose mode.',
    )
    parser.add_argument(
        '--no-vagrant',
        action='store_true',
        help='Always run, even if not inside the vagrant virtual machine.',
    )
    parser.add_argument(
        '--log-file',
        action='store_true',
        help='Send all output to a log file instead of to standard streams.',
    )
    parser.add_argument(
        '--output',
        type=Path,
        help=f'Optional log file. If specified, logs will be output to the '
             f'log file instead of standard streams. The path may be absolute '
             f'or relative to the logging directory `{LOGGING_DIR}`.',
    )

    # Configure subcommands.
    subparsers = parser.add_subparsers(
        dest='subcommand',
        title='synth subcommands',
        description='These commands expose portions of '
                    'Synth\'s functionality.',
        help='Run one of these commands to get started.',
        required=True,
    )
    analyze.init_subparser(subparsers)
    build_image.init_subparser(subparsers)
    datasets.init_subparser(subparsers)
    experiments.init_subparser(subparsers)
    synthesize.init_subparser(subparsers)

    # Set defaults.
    parser.set_defaults(run=_default_subcommand_run)
    parser.set_defaults(time_str=time.strftime('%Y-%m-%dT%H:%M:%S'))

    # Parse arguments.
    args = parser.parse_args()

    # Configure file logging.
    if args.log_file:
        if args.output:
            log_file_path = LOGGING_DIR / args.output
        else:
            log_file_path = LOGGING_DIR / f'{args.time_str}.log'

        fd = open(log_file_path, 'a')
        sys.stdout = fd
        sys.stderr = fd
        install_stream_handler(stream=fd)

    # Set log level. Logging is set to INFO by default. Verbose mode
    # enables DEBUG.
    if args.verbose == 0:
        set_level('INFO')
    elif args.verbose == 1:
        set_level('VERBOSE')
    elif args.verbose == 2:
        set_level('DEBUG')
    else:
        set_level('SPAM')

    # Run.
    try:
        if args.log_file:
            logger.info(
                f'Executing: synth {join(list(map(quote, sys.argv[1:])))}'
            )
        args.run(args)
    except Exception as e:  # noqa: B902
        logger.exception(e)
