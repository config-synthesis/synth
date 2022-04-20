"""Synth classes used with configuration script parsing."""


# Imports.
from dataclasses import dataclass, field
from typing import Optional

from docker.types import Mount

from synth.synthesis.classes import ConfigurationTask


@dataclass
class ParseResult:
    """The result of parsing a configuration script.

    Attributes
    ----------
    base_image : Optional[str]
        A base image that the configuration script builds off of.
    tasks : list[ConfigurationTask]
        All configuration tasks parsed from a script.
    mounts : list[Mount]
        All required mounts parsed from the script.
    """

    base_image: Optional[str] = field(default=None)
    tasks: list[ConfigurationTask] = field(default_factory=list)
    mounts: list[Mount] = field(default_factory=list)
