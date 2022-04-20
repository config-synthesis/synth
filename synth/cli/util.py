"""Synth cli utilities."""


# Imports.
from argparse import Namespace
from functools import wraps
from typing import Optional

from synth.cli.typing import RunFunction
from synth.paths import ETC_SYNTH


NO_VAGRANT = ETC_SYNTH / 'no_vagrant'


def parse_bool(value: str) -> Optional[bool]:
    """Parse a human readable value as a bool.

    Note that this function differs from ``bool(value)`` because it attempts to
    interpret the value of the string. Recognized input values are:

    - yes/no
    - y/n
    - true/false
    - 1/0

    Parsing is case insensitive.

    Parameters
    ----------
    value : str
        Input value.

    Returns
    -------
    Optional[bool]
        The boolean value of the input string, or None if the string cannot be
        interpreted as a bool.
    """
    value = value.casefold()
    if value in ('yes', 'y', 'true', '1'):
        return True
    elif value in ('no', 'n', 'false', '0'):
        return False
    else:
        return None


def prompt_no_vagrant(func: RunFunction) -> RunFunction:
    """CLI decorator that prompts the user when not run inside vagrant.

    This is intended as a safeguard for commands like the stress test
    experiments that are likely to crash docker and/or consume lots of
    resources.

    Running with ``--no-vagrant`` explicitly disables this.

    Parameters
    ----------
    func : RunFunction
        CLI run function.

    Returns
    -------
    RunFunction
        Wrapped run function that will prompt the user when not running in
        the vagrant virtual machine.
    """

    @wraps(func)
    def wrapper(args: Namespace):
        """Prompt the user if not running in vagrant, then run.

        Parameters
        ----------
        args : Namespace
            CLI args.
        """
        if not getattr(args, 'no_vagrant', False) and not NO_VAGRANT.exists():
            # Prompt the user until we get a recognizable answer.
            print(
                'You do not appear to be running in the vagrant virtual '
                'machine, are you sure you wish to continue?',
            )
            response = input('[y/n]: ')
            proceed = parse_bool(response)
            while proceed is None:
                response = input('Unrecognized input, please answer [y/n]: ')
                proceed = parse_bool(response)

            # Exit if the user chose not to proceed.
            if not proceed:
                print('Exiting.')
                exit()

        func(args)

    return wrapper
