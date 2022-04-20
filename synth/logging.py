"""Synth logging."""


# Imports
import inspect
import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, Iterable, MutableMapping, Union

import coloredlogs

from synth.paths import BASE_DIR, WORKING_DIR


# Create the logging output directory.
LOGGING_DIR = WORKING_DIR / 'logs'
LOGGING_DIR.mkdir(exist_ok=True, parents=True)
LOGGING_FMT = (
    '%(asctime)s %(hostname)s[%(process)d] %(name)s %(levelname)8s %(message)s'
)
SUCCESS = 35
NOTICE = 25
VERBOSE = 15
SPAM = 5
logging.addLevelName(SUCCESS, 'SUCCESS')
logging.addLevelName(NOTICE, 'NOTICE')
logging.addLevelName(VERBOSE, 'VERBOSE')
logging.addLevelName(SPAM, 'SPAM')


class IndentedLoggingAdapter(logging.LoggerAdapter):
    """Indented logging adapter.

    This auto-indents your log messages based on the current frame.
    """

    # Frame constants.
    # Logger frames is the number of frames in the framelist that are from the
    # logging module when the current frame is for Logger.process.
    LOGGER_FRAMES = 3

    def __init__(self,
                 logger: logging.Logger,
                 extra: dict[Any, Any],
                 indent_file_mask: Iterable[str] = frozenset(),
                 level: Union[int, str] = logging.WARNING):
        """Initialize the logging adapter.

        Parameters
        ----------
        logger : logging.Logger
            Logger to adapt.
        extra : dict[Any, Any]
            Extra arguments.
        indent_file_mask : Set[str]
            A set of files which will not add to the message indentation
            if they show up in the outer frames. If a directory is specified,
            the mask will apply to all files and subdirectories. Files may be
            absolute or relative to the project root (BASE_DIR).
        level : Union[int, str]
            Default logging level. Will be WARNING if not provided.
        """
        super().__init__(logger, extra)
        self.logger.setLevel(level)
        self.file_mask = set(map(
            lambda m: str((BASE_DIR / m).resolve()),
            indent_file_mask,
        ))
        self.cwd = Path(os.getcwd())
        self.framelist: list[tuple[int, tuple[str, str]]] = []
        self.base_indent = 0

    def _not_masked(self, filename: str) -> bool:
        """Determine if a filename has been masked.

        Parameters
        ----------
        filename : str
            Filename to check.

        Returns
        -------
        bool
            True if the file has not been masked.
        """
        # Resolve filename from working directory. This does nothing if
        # filename is already absolute.
        filename = str(self.cwd / filename)

        # Check to see if masked.
        for mask in self.file_mask:
            if filename.startswith(mask):
                return False
        return True

    def process(self,
                msg: str,
                kwargs: MutableMapping) -> tuple[Any, MutableMapping]:
        """Process a log message.

        Parameters
        ----------
        msg : str
            Log message.
        kwargs : dict[Any, Any]
            Extra arguments.

        Returns
        -------
        tuple[str, dict[Any, Any]]
            Processed message and extra arguments.
        """
        # Get all outer frames for the current frame. Note that this returns
        # frames in reverse order (most recent first, root last).
        framelist = list(filter(
            lambda f: self._not_masked(f.filename),
            inspect.getouterframes(inspect.currentframe())[
                self.LOGGER_FRAMES:
            ],
        ))

        # Number of times to indent the message.
        indent = self.base_indent

        # New framelist to save.
        newframelist = []

        # Process the common prefix between the old and current framelists.
        while self.framelist and framelist:

            # Get the current frame and take only the filename and function.
            _frame = framelist.pop()
            frame = (_frame.filename, _frame.function)

            # Get the old frame and whether or not it was logged.
            logged, oldframe = self.framelist.pop()

            # If the frames do not match, we're no longer on the common prefix.
            if frame != oldframe:
                framelist.append(_frame)
                break

            # Indent if the old frame had a log statement and save in the new
            # framelist.
            indent += logged
            newframelist.append((logged, oldframe))

        # If the current framelist is empty, that means it occurs on the path
        # of (or is the same as) the old framelist. In this case we've counted
        # one more indent than we actually want, so decrease by one. Otherwise,
        # add the remaining frames to the new framelist and note a log on the
        # last one.
        if not framelist:
            indent -= 1
        else:
            newframelist += [
                (0, (frameinfo.filename, frameinfo.function))
                for idx in range(len(framelist) - 1, 0, -1)
                if (frameinfo := framelist[idx])
            ]
            frameinfo = framelist[0]
            newframelist.append((1, (frameinfo.filename, frameinfo.function)))

        # Set the new framelist. Reversed so that it matches with getting
        # frames from inspect.
        self.framelist = list(reversed(newframelist))

        # Return message with potential indent.
        if indent > 0:
            return f'{"----" * (indent - 1)}---> {msg}', kwargs
        else:
            return msg, kwargs

    @contextmanager
    def indent(self) -> Generator[None, None, None]:
        """Indent a set of logs.

        Note that the logger automatically indents based on function calls.
        This is only necessary to indent logs within the same function call.

        Returns
        -------
        Generator[None, None, None]
            Generator for @contextmanager.
        """
        try:
            self.base_indent += 1
            yield
        finally:
            self.base_indent -= 1

    def success(self, msg: str, *args, **kwargs):
        """Log success."""
        self.log(SUCCESS, msg, *args, **kwargs)

    def notice(self, msg: str, *args, **kwargs):
        """Log notice."""
        self.log(NOTICE, msg, *args, **kwargs)

    def verbose(self, msg: str, *args, **kwargs):
        """Log verbose."""
        self.log(VERBOSE, msg, *args, **kwargs)

    def spam(self, msg: str, *args, **kwargs):
        """Log spam."""
        self.log(SPAM, msg, *args, **kwargs)


def install_stream_handler(**kwargs):
    """Install a coloredlogs stream handler.

    This acts like ``coloredlogs.install``, except that it preserves the
    logging level and format for the Synth logger by default.

    Additional kwargs are passed through to ``coloredlogs.install``.
    """
    _kwargs = {
        'logger': logger.logger,
        'level': logger.logger.level,
        'fmt': LOGGING_FMT,
    }
    _kwargs.update(kwargs)
    coloredlogs.install(**_kwargs)


def set_level(level: Any):
    """Set the logging level for the synth logger.

    Parameters
    ----------
    level : Any
        Logging level. Must be a valid value for ``logging.setLevel``.
    """
    # Set the main logging level.
    logger.setLevel(level)

    # Set logging level on the handler.
    handler, _ = coloredlogs.find_handler(
        logger.logger,
        coloredlogs.match_stream_handler,
    )
    if handler:
        handler.setLevel(level)


# Configure logging
logger = IndentedLoggingAdapter(
    logging.getLogger(__name__),
    {},
    indent_file_mask={
        '<frozen importlib._bootstrap>',
        '<frozen importlib._bootstrap_external>',
        'synth.py',
        'synth/cli',
    },
    level=logging.WARNING,
)
install_stream_handler()
