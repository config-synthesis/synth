"""Tests for Synth shell parsing."""


# Imports.
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent

import pytest

from synth.synthesis.classes import (
    ConfigurationSystem,
    ConfigurationTask,
    frozendict,
)
from synth.synthesis.configuration_scripts.shell import (
    parse_shell_script,
    write_shell_script,
)


class TestParseShellScript:
    """Tests for ``parse_shell_script``."""

    def test_ignores_comments(self):
        """Verify comments are ignored."""
        script = dedent("""
            #!/bin/bash
            exe1 \\
                arg1 arg2
                # Comment
            exe2 \\
                # Comment
                arg1 arg2
        """)

        result = parse_shell_script(script)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe1',
                arguments=('arg1', 'arg2'),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe2',
                arguments=('arg1', 'arg2'),
                changes=frozenset(),
            ),
        ]

    def test_parses_single_command(self):
        """Verify a single command can be parsed."""
        script = dedent("""
            #!/bin/bash
            touch file.txt
        """)

        result = parse_shell_script(script)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
        ]

    def test_parses_newline_delimited(self):
        """Verify commands on separate lines are parsed correctly."""
        script = dedent("""
            #!/bin/bash
            touch file.txt
            rm file.txt
        """)

        result = parse_shell_script(script)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='rm',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
        ]

    def test_parses_crlf_delimited(self):
        """Verify commands separated by CRLF are parsed correctly."""
        script = dedent("""
            #!/bin/bash
            touch file.txt\r\nrm file.txt
        """)

        result = parse_shell_script(script)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='rm',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
        ]

    def test_parses_semicolon_delimited(self):
        """Verify semicolon separated commands are parsed correctly."""
        script = dedent("""
            #!/bin/bash
            touch file.txt;rm file.txt
        """)

        result = parse_shell_script(script)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='rm',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
        ]

    def test_parses_single_ampersand_delimited(self):
        """Verify ampersand separated commands are parsed correctly."""
        script = dedent("""
            #!/bin/bash
            touch file.txt & rm file.txt
        """)

        result = parse_shell_script(script)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='rm',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
        ]

    def test_parses_double_ampersand_delimited(self):
        """Verify double ampersand separated commands are parsed correctly."""
        script = dedent("""
            #!/bin/bash
            touch file.txt && rm file.txt
        """)

        result = parse_shell_script(script)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='rm',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
        ]

    def test_parses_double_or_delimited(self):
        """Verify double or separated commands are parsed correctly."""
        script = dedent("""
            #!/bin/bash
            touch file.txt || rm file.txt
        """)

        result = parse_shell_script(script)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='rm',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
        ]

    def test_parses_escaped_newline(self):
        """Verify escaped newlines are ignored."""
        script = dedent("""
            #!/bin/bash
            touch file.txt \\
                && rm file.txt
        """)

        result = parse_shell_script(script)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='rm',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
        ]

    def test_multiple_multiline_run_commands(self):
        """Verify multiple multi-line run commands are parsed correctly."""
        script = dedent("""
            touch \\
                file.txt
            rm \\
                file.txt
        """)

        result = parse_shell_script(script)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='rm',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
        ]

    def test_multiple_multiline_run_commands_semicolon_delimited(self):
        """Verify multi-line run commands with `;` are parsed correctly."""
        script = dedent("""
            touch \\
                file.txt \\
            ;
            rm \\
                file.txt \\
            ;
        """)

        result = parse_shell_script(script)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='rm',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
        ]

    def test_ignores_whitespace(self):
        """Verify parts that are entirely whitespace are ignored."""
        # This string has extra whitespace after the \\.
        script = dedent("""
            #!/bin/bash
            touch file.txt \\         
                && rm file.txt
        """)  # noqa: W291

        result = parse_shell_script(script)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='rm',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
        ]

    def test_parses_all(self):
        """Verify everything parses well together."""
        script = dedent("""
            #!/bin/bash
            exe1
            exe2; exe3
            exe4 & exe5
            exe6 && exe7
            exe8 || exe9
            exe10 \\
                && exe11
                && exe12
        """)

        result = parse_shell_script(script)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable=f'exe{i}',
                arguments=(),
                changes=frozenset(),
            )
            for i in range(1, 13)
        ]

    def test_redirects(self):
        """Verify redirects are parsed correctly."""
        script = dedent("""
            echo 'line1' >> out.conf
            echo 'line2' >> out.conf
            echo 'line3' >> out.conf
        """)

        result = parse_shell_script(script)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='echo',
                arguments=('line1', '>>', 'out.conf'),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='echo',
                arguments=('line2', '>>', 'out.conf'),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='echo',
                arguments=('line3', '>>', 'out.conf'),
                changes=frozenset(),
            ),
        ]

    def test_vars(self):
        """Verify vars are parsed correctly."""
        script = dedent("""
            #!/bin/bash

            export VAR_1='VALUE 1'
            VAR_2='VALUE 2'

            echo "$VAR_1"
            echo "${VAR_2}"
            echo "${VAR_3:-DEFAULT}"
            VAR_4='VALUE 4' command
            echo "${VAR_4:-DEFAULT}"
        """)

        result = parse_shell_script(script)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='export',
                arguments=('VAR_1=VALUE 1',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable="declare",
                arguments=('VAR_2=VALUE 2',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='echo',
                arguments=('VALUE 1',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='echo',
                arguments=('VALUE 2',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='echo',
                arguments=('DEFAULT',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='declare',
                arguments=('VAR_4=VALUE 4',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='command',
                arguments=(),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='unset',
                arguments=('VAR_4',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='echo',
                arguments=('DEFAULT',),
                changes=frozenset(),
            ),
        ]

    def test_globs_are_not_expanded(self):
        """Verify file globs are not expanded, but variables are."""
        with TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)
            file1 = temp_dir_path / 'file1'
            file1.touch()

            script = dedent(f"""
                #!/bin/bash

                export DIR='{temp_dir}'
                echo $DIR/*
            """)

            result = parse_shell_script(script)

            assert result.tasks == [
                ConfigurationTask(
                    system=ConfigurationSystem.SHELL,
                    executable='export',
                    arguments=(f'DIR={temp_dir}',),
                    changes=frozenset(),
                ),
                ConfigurationTask(
                    system=ConfigurationSystem.SHELL,
                    executable="echo",
                    arguments=(f'{temp_dir}/*',),
                    changes=frozenset(),
                ),
            ]

    def test_quoted_arguments_with_spaces(self):
        """Verify quoted arguments with spaces are parsed correctly."""
        script = dedent("""
            #!/bin/bash

            exe 'argument1 with space' \\
                "argument2 with space"
        """)

        result = parse_shell_script(script)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe',
                arguments=(
                    'argument1 with space',
                    'argument2 with space',
                ),
                changes=frozenset(),
            ),
        ]


class TestWriteShellScript:
    """Tests for ``write_shell_script``."""

    def test_raises_if_not_shell(self):
        """Verify a ValueError is raise if any task is not a shell task."""
        tasks = [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('file',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.ANSIBLE,
                executable='ansible.builtin.file',
                arguments=frozendict({'name': 'file', 'state': 'touch'}),
                changes=frozenset(),
            ),
        ]

        with pytest.raises(ValueError):
            write_shell_script(tasks)

    def test_quotes_values(self):
        """Verify shell commands are properly quoted."""
        tasks = [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('value1 value2', '&arg2'),
                changes=frozenset(),
            ),
        ]

        script = write_shell_script(tasks)

        assert script == dedent("""
            #!/usr/bin/env bash

            touch $'value1 value2' $'&arg2'
        """).strip()

    def test_escapes_newlines(self):
        """Verify shell commands with newlines are properly escaped."""
        tasks = [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='echo',
                arguments=('-n', 'line1\nline2'),
                changes=frozenset(),
            ),
        ]

        script = write_shell_script(tasks)

        assert script == dedent("""
            #!/usr/bin/env bash

            echo -n $'line1\\nline2'
        """).strip()

    def test_write_script(self):
        """Verify a correct script is generated."""
        tasks = [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe1',
                arguments=(),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe2',
                arguments=('arg1',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe3',
                arguments=('arg1', 'arg2'),
                changes=frozenset(),
            ),
        ]

        script = write_shell_script(tasks)

        assert script == dedent("""
            #!/usr/bin/env bash

            exe1
            exe2 arg1
            exe3 arg1 arg2
        """).strip()
