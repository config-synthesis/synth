"""Build image subcommand."""


# Imports.
import json
from argparse import Namespace
from pathlib import Path

from synth.cli.typing import SubparsersAction
from synth.serverless.frameworks import Framework


def init_subparser(subparsers: SubparsersAction):
    """Initialize the subcommand parser.

    Parameters
    ----------
    subparsers : SubparsersAction
        A subparsers action from the parent parser. The init function will
        use this action to initialize a parser for the subcommand.
    """
    names = [f.name for f in Framework.ALL_FRAMEWORKS.values()]
    codes = [f.code for f in Framework.ALL_FRAMEWORKS.values()]
    parser = subparsers.add_parser(
        'build-image',
        help='Build a Docker image for a serverless function that has a '
             'CloudFormation or Serverless configuration.',
        description='',  # TODO
    )
    parser.add_argument(
        'path',
        type=Path,
        help=f'Path to a serverless function. This may be the path to a '
             f'directory containing a framework configuration, or a path to a '
             f'framework configuration file itself. Currently supported '
             f'frameworks are: {", ".join(names)}.',
    )
    parser.add_argument(
        '--framework',
        type=str,
        choices=codes,
        required=False,
        help='Specify the serverless framework to use.',
    )
    parser.add_argument(
        '--function',
        type=str,
        required=False,
        help='Identifier for the specific function to build, as defined by '
             'the configuration file. Specify this to build a non-default '
             'function, or if the framework being used does not support a '
             'default function.',
    )
    parser.set_defaults(run=run)


def run(args: Namespace):
    """Build a Docker image.

    Parameters
    ----------
    args : Namespace
        Command line arguments.
    """
    framework = Framework.for_path(
        path=args.path,
        code=args.framework,
    )
    image = framework.build_image(
        function_name=args.function,
    )
    print(json.dumps(image, indent=4))
