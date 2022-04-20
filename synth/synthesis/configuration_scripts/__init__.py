"""Synth configuration language parsing."""


# Imports
from collections.abc import Sequence
from pathlib import Path
from typing import Optional, Protocol, Union

from synth.synthesis.classes import ConfigurationSystem, ConfigurationTask
from synth.synthesis.configuration_scripts.ansible import (
    parse_ansible_playbook,
    write_playbook,
)
from synth.synthesis.configuration_scripts.classes import ParseResult
from synth.synthesis.configuration_scripts.docker import (
    parse_dockerfile,
    write_dockerfile,
)
from synth.synthesis.configuration_scripts.shell import (
    parse_shell_script,
    write_shell_script,
)


class Parser(Protocol):
    """A configuration script parser."""

    def __call__(self,
                 script: Union[Path, str],
                 /,
                 context: Optional[Path] = None) -> ParseResult:
        """Parse a configuration script.

        Parameters
        ----------
        script : Union[Path, str]
            A path to a configuration script or the script itself.
        context : Optional[Path]
            A path to the context directory for the script. If provided this
            may enable additional metadata from the parse.

        Returns
        -------
        ParseResult
            The result of parsing.
        """
        pass


class Writer(Protocol):
    """A configuration script writer."""

    def __call__(self, tasks: Sequence[ConfigurationTask]) -> str:
        """Write a configuration script.

        Parameters
        ----------
        tasks : Sequence[ConfigurationTask]
            A sequence of configuration tasks that should be in the final
            script.

        Returns
        -------
        str
            The source of the final configurations script.
        """
        pass


def get_parser(path: Path) -> Parser:
    """Get a configuration script parser.

    Support detection is based on the file name. No attempt is made to parse
    the file at the provided path.

    Parameters
    ----------
    path : Path
        Path to a configuration script.

    Raises
    ------
    ValueError
        Raised if the path does not exist or is not for a recognized type.

    Returns
    -------
    Parser
        A parser that accepts the path to the configuration file and returns
        a parse result.
    """
    if not path.exists():
        raise ValueError(f'Path does not exist: {path}.')

    if path.suffix.casefold() in ('.yml', '.yaml'):
        return parse_ansible_playbook
    if 'dockerfile' in path.name.casefold():
        return parse_dockerfile
    elif path.suffix.casefold() == '.sh':
        return parse_shell_script
    else:
        raise ValueError(f'Unknown file type: {path}.')


def get_writer(system: ConfigurationSystem) -> Writer:
    """Get a configuration script writer.

    Parameters
    ----------
    system : ConfigurationSystem
        Configuration system of the returned writer.

    Raises
    ------
    ValueError
        Raised if the configuration system is not recognized.

    Returns
    -------
    Writer
        A writer that accepts a sequence of configuration tasks and returns
        the contents of a configuration script in the desired language.
    """
    if system == ConfigurationSystem.SHELL:
        return write_shell_script
    elif system == ConfigurationSystem.DOCKER:
        return write_dockerfile
    elif system == ConfigurationSystem.ANSIBLE:
        return write_playbook
    else:
        raise ValueError(f'Unknown System `{system}`.')


def get_default_name(system: ConfigurationSystem,
                     suffix: Optional[str] = None) -> str:
    """Get the default name for a configuration system script.

    Parameters
    ----------
    system : ConfigurationSystem
        Target system for the script.
    suffix : Optional[str]
        Optional suffix to be added to the end of the configuration script
        name. How this is handled varies by system, but the suffix is normally
        appended to the default name before the file extension.

    Returns
    -------
    str
        Configuration script name.
    """
    if system == ConfigurationSystem.ANSIBLE:
        if suffix:
            return f'playbook-{suffix}.yml'
        else:
            return 'playbook.yml'
    elif system == ConfigurationSystem.DOCKER:
        if suffix:
            return f'Dockerfile.{suffix}'
        else:
            return 'Dockerfile'
    elif system == ConfigurationSystem.SHELL:
        if suffix:
            return f'configuration-script-{suffix}.sh'
        else:
            return 'configuration-script.sh'
    else:
        raise ValueError(f'Unknown System `{system}`.')
