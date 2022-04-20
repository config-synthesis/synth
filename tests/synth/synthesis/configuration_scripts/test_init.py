"""Tests for Synth parsing."""


# Imports.
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest

from synth.synthesis.classes import ConfigurationSystem
from synth.synthesis.configuration_scripts import get_parser, get_writer
from synth.synthesis.configuration_scripts.ansible import (
    parse_ansible_playbook,
    write_playbook,
)
from synth.synthesis.configuration_scripts.docker import (
    parse_dockerfile,
    write_dockerfile,
)
from synth.synthesis.configuration_scripts.shell import (
    parse_shell_script,
    write_shell_script,
)


class TestGetParser:
    """Tests for ``get_parser``."""

    def test_get_not_exists(self):
        """Verify ValueError is raised if the path does not exist."""
        with pytest.raises(ValueError):
            get_parser(Path('/tmp/does-not-exist'))  # noqa: S108

    def test_get_unrecognized(self):
        """Verify ValueError is raised if the path is not recognized."""
        with NamedTemporaryFile(suffix='.gz') as file:
            with pytest.raises(ValueError):
                get_parser(Path(file.name))

    def test_get_yml(self):
        """Verify .yml files are parsed as Ansible."""
        with NamedTemporaryFile(suffix='.yml') as file:
            assert get_parser(Path(file.name)) == parse_ansible_playbook

    def test_get_yaml(self):
        """Verify .yaml files are parsed as Ansible."""
        with NamedTemporaryFile(suffix='.yaml') as file:
            assert get_parser(Path(file.name)) == parse_ansible_playbook

    def test_get_dockerfile(self):
        """Verify Dockerfiles are parsed as Docker."""
        with TemporaryDirectory() as dir_name:
            dir_path = Path(dir_name)
            file_path = dir_path / 'Dockerfile'
            file_path.touch()
            assert get_parser(file_path) == parse_dockerfile

    def test_get_dockerfile_build(self):
        """Verify Dockerfiles with suffixes are parsed as Docker."""
        with TemporaryDirectory() as dir_name:
            dir_path = Path(dir_name)
            file_path = dir_path / 'Dockerfile.build'
            file_path.touch()
            assert get_parser(file_path) == parse_dockerfile

    def test_get_dockerfile_bullseye(self):
        """Verify Dockerfiles with longer names are parsed as Docker."""
        with TemporaryDirectory() as dir_name:
            dir_path = Path(dir_name)
            file_path = dir_path / 'Dockerfile-bullseye'
            file_path.touch()
            assert get_parser(file_path) == parse_dockerfile

    def test_get_sh(self):
        """Verify .sh files are parsed as shell."""
        with NamedTemporaryFile(suffix='.sh') as file:
            assert get_parser(Path(file.name)) == parse_shell_script


class TestGetWriter:
    """Tests for ``get_writer``."""

    def test_get_shell(self):
        """Verify getting a shell script writer."""
        assert get_writer(ConfigurationSystem.SHELL) == write_shell_script

    def test_get_docker(self):
        """Verify getting a Dockerfile writer."""
        assert get_writer(ConfigurationSystem.DOCKER) == write_dockerfile

    def test_get_ansible(self):
        """Verify getting an Ansible writer."""
        assert get_writer(ConfigurationSystem.ANSIBLE) == write_playbook
