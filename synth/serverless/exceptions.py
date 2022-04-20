"""Synth serverless exceptions."""


# Imports.
from synth.exceptions import ClassDefinitionException, SynthException


class FrameworkException(SynthException):
    """Exceptions related to serverless frameworks."""


class FrameworkDefinitionException(FrameworkException,
                                   ClassDefinitionException):
    """A framework class definition is not correct."""


class InvalidFrameworkConfigFileException(FrameworkException):
    """A framework configuration file is not valid."""


class MissingFrameworkConfigFileException(FrameworkException):
    """A serverless function does not have a configuration file."""


class NoDefaultFunctionException(FrameworkException):
    """A serverless configuration does not specify a default function."""


class NoSuchFunctionException(FrameworkException):
    """A specified serverless function does not exist."""


class UnknownFrameworkException(FrameworkException):
    """Exception indicating an unknown framework."""
