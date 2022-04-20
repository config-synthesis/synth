"""Tests for Synth Dockerfile parsing."""


# Imports.
from textwrap import dedent

import pytest
from docker.types import Mount

from synth.synthesis.classes import (
    ConfigurationSystem,
    ConfigurationTask,
    frozendict,
)
from synth.synthesis.configuration_scripts.docker import (
    parse_dockerfile,
    write_dockerfile,
)


class TestParseDockerfile:
    """Tests for ``parse_dockerfile``."""

    def test_single_command_exec_syntax(self):
        """Verify a single command in the exec syntax is parsed correctly."""
        dockerfile = dedent("""
            RUN ["touch", "file.txt"]
        """)

        result = parse_dockerfile(dockerfile)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
        ]

    def test_multiline_command_exec_syntax(self):
        """Verify a multiline command in exec syntax is parsed correctly."""
        dockerfile = dedent("""
            RUN ["touch", \\
                "file.txt"]
        """)

        result = parse_dockerfile(dockerfile)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
        ]

    def test_single_command_shell_syntax(self):
        """Verify a single command in shell syntax is parsed correctly."""
        dockerfile = dedent("""
            RUN touch file.txt
        """)

        result = parse_dockerfile(dockerfile)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
        ]

    def test_multiline_command_shell_syntax(self):
        """Verify a multiline command in shell syntax is parsed correctly."""
        dockerfile = dedent("""
            RUN touch \\
                file.txt
        """)

        result = parse_dockerfile(dockerfile)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('file.txt',),
                changes=frozenset(),
            ),
        ]

    def test_multiple_commands_shell_syntax(self):
        """Verify multiple commands in shell syntax are parsed correctly."""
        dockerfile = dedent("""
            RUN touch file.txt \\
                && rm file.txt
        """)

        result = parse_dockerfile(dockerfile)

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

    def test_multiple_run_commands(self):
        """Verify multiple run commands are parsed correctly."""
        dockerfile = dedent("""
            RUN touch file.txt
            RUN rm file.txt
        """)

        result = parse_dockerfile(dockerfile)

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
        dockerfile = dedent("""
            RUN touch \\
                file.txt
            RUN rm \\
                file.txt
        """)

        result = parse_dockerfile(dockerfile)

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
        dockerfile = dedent("""
            RUN touch \\
                file.txt \\
            ;
            RUN rm \\
                file.txt \\
            ;
        """)

        result = parse_dockerfile(dockerfile)

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

    def test_parse_with_noise(self):
        """Verify only RUN commands are parsed."""
        dockerfile = dedent("""
            FROM base
            RUN touch file.txt
            EXPOSE 8080
            RUN rm file.txt
            CMD /bin/exe
        """)

        result = parse_dockerfile(dockerfile)

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

    def test_redirects(self):
        """Verify redirects are parsed correctly."""
        dockerfile = dedent("""
            RUN echo 'line1' >> out.conf
            RUN echo 'line2' >> out.conf
            RUN echo 'line3' >> out.conf
        """)

        result = parse_dockerfile(dockerfile)

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
        dockerfile = dedent("""
            ENV VAR_1='VALUE 1'
            ARG VAR_2='VALUE 2'
            RUN VAR_3='VALUE 3'

            RUN echo "$VAR_1"
            RUN echo "${VAR_2}"
            RUN echo "${VAR_3:-DEFAULT}"
            RUN echo "${VAR_4:-DEFAULT}"
        """)

        result = parse_dockerfile(dockerfile)

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
                executable="declare",
                arguments=('VAR_3=VALUE 3',),
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
                arguments=('VALUE 3',),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='echo',
                arguments=('DEFAULT',),
                changes=frozenset(),
            ),
        ]

    def test_mounts(self):
        """Verify mounts are returned correctly."""
        dockerfile = dedent("""
            COPY /file1 /dir1/file1
            ADD /file2 /dir2/file2
            COPY /file3 /file4 /dir3
        """)

        result = parse_dockerfile(dockerfile)

        assert result.mounts == [
            Mount(
                source='/file1',
                target='/dir1/file1',
                type='bind',
                read_only=True,
            ),
            Mount(
                source='/file2',
                target='/dir2/file2',
                type='bind',
                read_only=True,
            ),
            Mount(
                source='/file3',
                target='/dir3',
                type='bind',
                read_only=True,
            ),
            Mount(
                source='/file4',
                target='/dir3',
                type='bind',
                read_only=True,
            ),
        ]

    def test_ignores_comments(self):
        """Verify comments are ignored."""
        dockerfile = dedent("""
            # Comment 1
            RUN exe1 \\
                && exe2 \\
                # Comment 2
                && exe3
        """)

        result = parse_dockerfile(dockerfile)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe1',
                arguments=(),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable="exe2",
                arguments=(),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable="exe3",
                arguments=(),
                changes=frozenset(),
            ),
        ]

    def test_parses_base_image(self):
        """Verify the base image is parsed correctly."""
        dockerfile = dedent("""
            FROM scratch
            FROM debian:11
        """)

        result = parse_dockerfile(dockerfile)

        assert result.base_image == 'debian:11'

    def test_ignores_stage_name(self):
        """Verify stage names are ignored when parsing base images."""
        dockerfile = dedent("""
            FROM debian:11-slim as installer
        """)

        result = parse_dockerfile(dockerfile)

        assert result.base_image == 'debian:11-slim'


class TestWriteDockerfile:
    """Tests for ``write_dockerfile``."""

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
            write_dockerfile(tasks)

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

        dockerfile = write_dockerfile(tasks)

        assert dockerfile == dedent("""
            FROM debian:11

            RUN touch $'value1 value2' $'&arg2'
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

        dockerfile = write_dockerfile(tasks)

        assert dockerfile == dedent("""
            FROM debian:11

            RUN echo -n $'line1\\nline2'
        """).strip()

    def test_write_dockerfile(self):
        """Verify a correct Dockerfile is generated."""
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

        dockerfile = write_dockerfile(tasks)

        assert dockerfile == dedent("""
            FROM debian:11

            RUN exe1
            RUN exe2 arg1
            RUN exe3 arg1 arg2
        """).strip()
