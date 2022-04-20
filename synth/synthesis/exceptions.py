"""Synth synthesis exceptions."""


# Imports.
from synth.exceptions import SynthException


class SynthesisException(SynthException):
    """A generic exception for synthesis errors."""


class MatchingException(SynthesisException):
    """A configuration task argument mapping is an invalid matching."""


class ParseException(SynthesisException):
    """Unable to parse a configuration script."""


class SearchError(SynthesisException):
    """Synthesis search has failed to find a result."""


class UnresolvedTaskFailure(SearchError):
    """Synthesis search has failed to resolve a task error."""


class DockerException(SynthException):
    """An exception happened with Docker."""
