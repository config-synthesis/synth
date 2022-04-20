"""Synth timeout utilities."""


# Imports.
import signal
from types import FrameType, TracebackType

from synth.logging import logger


class Timeout:
    """Timeout class.

    Taken from StackOverflow: https://stackoverflow.com/a/22348885
    """

    def __init__(self, seconds: int = 1, error_message: str = 'Timeout'):
        """Create a new timeout context.

        Parameters
        ----------
        seconds : int
            Number of seconds before the timeout.
        error_message : str
            Error message when a timeout occurs.
        """
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum: int, frame: FrameType):
        """Handle a timeout.

        Parameters
        ----------
        signum : int
            Number of the signal raised.
        frame : FrameType
            The current frame.
        """
        logger.debug('Timeout encountered')
        raise TimeoutError(self.error_message)

    def __enter__(self):
        """Enter the timeout context."""
        signal.signal(signal.SIGALRM, self.handle_timeout)
        time_remaining = signal.alarm(self.seconds)
        if time_remaining:
            raise RuntimeError(
                'Attempting to enter a second timeout context while one is '
                'still active.'
            )

    def __exit__(self,
                 exe_type: type,
                 exe_value: Exception,
                 exe_traceback: TracebackType):
        """Exit the timeout context.

        Parameters
        ----------
        exe_type : type
            The type of exception raised, if any.
        exe_value : Exception
            The value of the exception raised, if any.
        exe_traceback : TracebackType
            The traceback if an exception was raised.
        """
        signal.alarm(0)
