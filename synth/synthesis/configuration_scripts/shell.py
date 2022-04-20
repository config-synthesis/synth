"""Synth shell script parsing."""


# Imports
from collections.abc import Mapping, Sequence
from pathlib import Path
from shlex import shlex
from typing import Optional, Union

import sh
from sh import ErrorReturnCode

from synth.synthesis.classes import ConfigurationSystem, ConfigurationTask
from synth.synthesis.configuration_scripts.classes import ParseResult
from synth.util.shell import join, quote


# Constants
PUNCTUATION_CHARS = set('();|&\r\n')
WHITESPACE = ' \t'


def _replace_vars(s: str,
                  env_vars: Mapping[str, str],
                  shell_vars: Mapping[str, str]) -> str:
    """Use the shell to perform variable expansion.

    Parameters
    ----------
    s : str
        Input string.
    env_vars : Mapping[str, str]
        Environment variables.
    shell_vars : Mapping[str, str]
        Shell variables.

    Returns
    -------
    str
        ``s`` with any available vars expanded.
    """
    env = {}
    env.update(env_vars)
    env.update(shell_vars)

    # Special case for echo, which can't escape bare flags.
    if s == '-n':
        return s

    # Use the shlex.quote instead of synth.util.shell.quote because we need
    # shell redirection and special chars to be quoted.
    try:
        s = s.replace('*', '"*"')
        proc = sh.bash('-c', f'echo -n {quote(s)}', _env=env)
        return str(proc.stdout, encoding='utf-8')
    except ErrorReturnCode:
        return s


def _parse_tasks(cmd: Sequence[str],
                 env_vars: Mapping[str, str],
                 shell_vars: Mapping[str, str],
                 ) -> tuple[list[ConfigurationTask],
                            dict[str, str],
                            dict[str, str]]:
    """Parse configuration tasks from a shell command.

    Parameters
    ----------
    cmd : Sequence[str]
        Shell command.
    env_vars : Mapping[str, str]
        Environment variables.
    shell_vars : Mapping[str, str]
        Shell variables.

    Returns
    -------
    list[ConfigurationTask]
        All configuration tasks.
    env_vars : Mapping[str, str]
        Environment variables.
    shell_vars : Mapping[str, str]
        Shell variables.
    """
    cmd = list(cmd)
    env_vars = dict(env_vars)
    shell_vars = dict(shell_vars)

    if not cmd:
        return [], env_vars, shell_vars

    tasks = []
    new_shell_vars = set()

    # Process all variable assignments. These will be converted into individual
    # declare tasks prior to the resulting command.
    while cmd and '=' in cmd[0]:
        assignment = cmd.pop(0)
        assignment = _replace_vars(
            assignment,
            env_vars=env_vars,
            shell_vars=shell_vars,
        )

        key, value = assignment.split('=', maxsplit=1)
        shell_vars[key] = value
        new_shell_vars.add(key)

        tasks.append(ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='declare',
            arguments=(assignment,),
            changes=frozenset(),
        ))

    # Process the final command, if one exists.
    if cmd:
        cmd = [
            _replace_vars(v, env_vars=env_vars, shell_vars=shell_vars)
            for v in cmd
        ]

        executable = cmd[0]
        arguments = cmd[1:]

        # If the command is export, set environment variables.
        # If the command is unset, unset the environment and shell variables.
        if executable == 'export':
            for arg in arguments:
                if '=' in arg:
                    key, value = arg.split('=', maxsplit=1)
                    env_vars[key] = value
                elif arg in shell_vars:
                    env_vars[arg] = shell_vars[arg]
        elif executable == 'unset':
            for arg in arguments:
                if arg in env_vars:
                    del env_vars[arg]
                if arg in shell_vars:
                    del shell_vars[arg]

        tasks.append(ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable=executable,
            arguments=tuple(arguments),
            changes=frozenset(),
        ))

        # If any shell variables were set inline with a command, they last only
        # for the duration of the command. Unset them afterwards.
        for var in new_shell_vars:
            tasks.append(ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='unset',
                arguments=(var,),
                changes=frozenset(),
            ))
            if var in env_vars:
                del env_vars[var]
            if var in shell_vars:
                del shell_vars[var]

    return tasks, env_vars, shell_vars


def parse_shell_script(script: Union[Path, str],
                       context: Optional[Path] = None) -> ParseResult:
    """Parse a shell script as a list of configuration tasks.

    Parameters
    ----------
    script : Union[Path, str]
        The shell script to parse. This should either be the script text or a
        path to a readable file.
    context : Optional[Path]
        Path to an optional context directory. If specified, it will be used
        for additional metadata in the parse result.

    Returns
    -------
    ParseResult
        Configuration tasks and metadata parsed from the script.
    """
    if isinstance(script, Path):
        script = script.read_text()

    # Remove any escaped newlines.
    script = script.replace('\\\n', '')

    lexer = shlex(script, posix=True, punctuation_chars=PUNCTUATION_CHARS)
    lexer.whitespace = WHITESPACE
    lexer.whitespace_split = True

    tasks = []
    env_vars = {}
    shell_vars = {}
    cmd = []
    for part in lexer:
        if all(v in PUNCTUATION_CHARS for v in part):
            _tasks, env_vars, shell_vars = _parse_tasks(
                cmd,
                env_vars,
                shell_vars,
            )
            tasks.extend(_tasks)
            cmd = []
        else:
            if not all(v in WHITESPACE for v in part):
                cmd.append(part)
    _tasks, env_vars, shell_vars = _parse_tasks(cmd, env_vars, shell_vars)
    tasks.extend(_tasks)

    return ParseResult(tasks=tasks)


def write_shell_script(tasks: Sequence[ConfigurationTask]) -> str:
    """Write a shell script from a sequence of configuration tasks.

    Parameters
    ----------
    tasks : Sequence[ConfigurationTask]
        Configuration tasks for the shell script.

    Returns
    -------
    str
        Shell script contents.
    """
    if any(task.system != ConfigurationSystem.SHELL for task in tasks):
        raise ValueError(
            'Cannot write shell script. All tasks must be shell tasks',
        )

    parts = [
        '#!/usr/bin/env bash',
        '',
        *(
            join([task.executable, *task.arguments])
            for task in tasks
        )
    ]
    return '\n'.join(parts)
