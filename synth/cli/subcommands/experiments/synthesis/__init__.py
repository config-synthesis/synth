"""Synth synthesis experiments CLI."""


# Imports.
from synth.cli.subcommands.experiments.synthesis import (
    debian_dockerfiles,
    debian_playbooks,
)
from synth.cli.typing import SubparsersAction


def init_subparser(subparsers: SubparsersAction):
    """Initialize the subcommand parser.

    Parameters
    ----------
    subparsers : SubparsersAction
        A subparsers action from the parent parser. The init function will
        use this action to initialize a parser for the subcommand.
    """
    parser = subparsers.add_parser(
        'synthesis',
        help='Run Synth synthesis experiments.',
    )
    subparsers = parser.add_subparsers(
        dest='subcommand',
        title='synth synthesis experiment subcommands',
        description='Run Synth synthesis experiments.',
        help='Run one of these commands to get started.',
        required=True,
    )
    debian_dockerfiles.init_subparser(subparsers)
    debian_playbooks.init_subparser(subparsers)
