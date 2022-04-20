"""Synthesize subcommand."""


# Imports.
import json
from argparse import Namespace

from docker import client
from requests.exceptions import ConnectionError

from synth.cli.typing import SubparsersAction
from synth.logging import logger
from synth.synthesis import synthesize_configuration_script
from synth.synthesis.classes import ConfigurationSystem
from synth.synthesis.serialization import from_dict


def init_subparser(subparsers: SubparsersAction):
    """Initialize the subcommand parser.

    Parameters
    ----------
    subparsers : SubparsersAction
        A subparsers action from the parent parser. The init function will
        use this action to initialize a parser for the subcommand.
    """
    parser = subparsers.add_parser(
        'synthesize',
        help='Synthesize an environment configuration.',
        description='Synthesize an environment configuration script that '
                    'either reproduces an existing environment or enables '
                    'the executability of a script.',
    )
    parser.add_argument(
        '--change',
        type=str,
        action='append',
        default=[],
        help='A manual change specification. May be specified in addition to '
             'other change sources.'
    )
    parser.add_argument(
        '--docker-image',
        help='A local Docker image to use as the source for changes.',
    )
    parser.add_argument(
        '--docker-base-image',
        default='debian:11',
        help='The base Docker image used to compute changes for '
             '--docker-image.',
    )
    parser.add_argument(
        '--system',
        type=ConfigurationSystem,
        choices={i.value for i in ConfigurationSystem},
        default=ConfigurationSystem.SHELL,
        help=f'The configuration system to use. '
             f'Defaults to `{ConfigurationSystem.SHELL}`.',
    )
    parser.add_argument(
        '--no-order',
        action='store_true',
        help='If provided, the satisfying configuration task set will be '
             'returned without attempting to order (and validate) them.',
    )
    parser.set_defaults(run=run)


def run(args: Namespace):
    """Synthesize a configuration script.

    Parameters
    ----------
    args : Namespace
        Command line arguments.
    """
    # Check for Docker if ordering is required.
    if not args.no_order:
        try:
            client.from_env().info()
        except ConnectionError:
            logger.exception(
                'Cannot connect to Docker. Please make sure Docker is '
                'available before running.'
            )
            exit(1)

    # Get changes.
    changes = set(map(
        lambda o: json.loads(o, object_hook=from_dict),
        args.change,
    ))

    # Synthesize a configuration script and print it to stdout.
    print(synthesize_configuration_script(
        system=args.system,
        image=args.docker_image,
        changes=changes,
        order=not args.no_order,
        base_image=args.docker_base_image,
        is_runner_image=not args.docker_base_image,
    ))
