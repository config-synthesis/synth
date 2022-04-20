"""Types for the CLI."""


# Imports.
from argparse import ArgumentParser, Namespace
from typing import Any, Callable, Protocol


RunFunction = Callable[[Namespace], None]


class SubparsersAction(Protocol):
    """SubparsersAction.

    This protocol is defined because argparse._SubparsersAction is not
    exported.
    """

    def add_parser(self, name: Any, **kwargs) -> ArgumentParser:
        """Add a new parser.

        Parameters
        ----------
        name
            Parser name.

        Returns
        -------
        ArgumentParser
            The new argument parser.
        """
        raise NotImplementedError('protocol method')
