"""Tests for Synth Ansible parsing."""


# Imports.
from textwrap import dedent

import pytest

from synth.synthesis.classes import (
    ConfigurationSystem,
    ConfigurationTask,
    frozendict,
)
from synth.synthesis.configuration_scripts.ansible import (
    parse_ansible_playbook,
    write_playbook,
)


class TestParseAnsiblePlaybook:
    """Tests for ``parse_ansible_playbook``."""

    def test_parses_strings(self):
        """Verify strings are parsed as the python ``str`` class."""
        playboook = dedent("""
            - hosts: all
              tasks:
              - ansible.builtin.file:
                  arg1: value1
                  arg2:
                    - value2.1
                    - value2.2
                  arg3:
                    key3.1: value3.1
                    key3.2: value3.2
        """)

        result = parse_ansible_playbook(playboook)
        task = result.tasks[0]

        assert type(task.executable) == str
        assert type(task.arguments['arg1']) == str
        assert all(
            type(value) == str
            for value in task.arguments['arg2']
        )
        assert all(
            type(key) == str
            for key in task.arguments['arg3'].keys()
        )
        assert all(
            type(value) == str
            for value in task.arguments['arg3'].values()
        )

    def test_parse_playbook(self):
        """Verify a playbook parses correctly."""
        playboook = dedent("""
            - hosts: all
              tasks:
              - ansible.builtin.file:
                  name: file.txt
                  state: absent
              - ansible.builtin.service:
                  name: nginx
                  state: started
        """)

        result = parse_ansible_playbook(playboook)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.ANSIBLE,
                executable='ansible.builtin.file',
                arguments=frozendict({'name': 'file.txt', 'state': 'absent'}),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.ANSIBLE,
                executable='ansible.builtin.service',
                arguments=frozendict({'name': 'nginx', 'state': 'started'}),
                changes=frozenset(),
            ),
        ]

    def test_vars(self):
        """Verify vars are parsed correctly."""
        playboook = dedent("""
            - hosts: all
              vars:
                VAR_1: VALUE 1
                VAR_2: "INCLUDES {{ VAR_1 }}"
              tasks:
              - ansible.builtin.file:
                  name: "{{ VAR_1 }}"
                  state: absent
              - ansible.builtin.service:
                  name: "{{ VAR_2 }}"
                  state: started
        """)

        result = parse_ansible_playbook(playboook)

        assert result.tasks == [
            ConfigurationTask(
                system=ConfigurationSystem.ANSIBLE,
                executable='ansible.builtin.file',
                arguments=frozendict({'name': 'VALUE 1', 'state': 'absent'}),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.ANSIBLE,
                executable='ansible.builtin.service',
                arguments=frozendict({
                    'name': 'INCLUDES VALUE 1',
                    'state': 'started',
                }),
                changes=frozenset(),
            ),
        ]

    def test_mounts(self):
        """Verify mounts are returned correctly."""
        playboook = dedent("""
            - hosts: all
              tasks:
              - ansible.builtin.file:
                  name: file
                  state: absent
        """)

        result = parse_ansible_playbook(playboook)

        assert result.mounts == []


class TestWriteAnsiblePlaybook:
    """Tests for ``write_ansible_playbook``."""

    def test_raises_if_not_ansible(self):
        """Verify a ValueError is raise if any task is not an Ansible task."""
        tasks = [
            ConfigurationTask(
                system=ConfigurationSystem.ANSIBLE,
                executable='ansible.builtin.file',
                arguments=frozendict({'name': 'file', 'state': 'touch'}),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='touch',
                arguments=('file',),
                changes=frozenset(),
            ),
        ]

        with pytest.raises(ValueError):
            write_playbook(tasks)

    def test_write_playbook(self):
        """Verify a correct playbook is generated."""
        tasks = [
            ConfigurationTask(
                system=ConfigurationSystem.ANSIBLE,
                executable='exe1',
                arguments=frozendict(),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.ANSIBLE,
                executable='exe2',
                arguments=frozendict({'key1': 'value1'}),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.ANSIBLE,
                executable='exe3',
                arguments=frozendict({'key1': 'value1', 'key2': 'value2'}),
                changes=frozenset(),
            ),
        ]

        playbook = write_playbook(tasks)

        assert playbook == dedent("""
            - become: true
              hosts: localhost
              tasks:
              - exe1: {}
                name: Run exe1
              - exe2:
                  key1: value1
                name: Run exe2
              - exe3:
                  key1: value1
                  key2: value2
                name: Run exe3
        """).lstrip()

    def test_write_host(self):
        """Verify a correct playbook is generated."""
        host = 'hostname'
        tasks = [
            ConfigurationTask(
                system=ConfigurationSystem.ANSIBLE,
                executable='exe1',
                arguments=frozendict(),
                changes=frozenset(),
            ),
        ]

        playbook = write_playbook(tasks, hosts=host)

        assert playbook == dedent(f"""
            - become: true
              hosts: {host}
              tasks:
              - exe1: {{}}
                name: Run exe1
        """).lstrip()

    def test_set_become(self):
        """Verify become is set."""
        tasks = [
            ConfigurationTask(
                system=ConfigurationSystem.ANSIBLE,
                executable='exe1',
                arguments=frozendict(),
                changes=frozenset(),
            ),
        ]

        playbook = write_playbook(tasks, become=False)

        assert playbook == dedent("""
            - become: false
              hosts: localhost
              tasks:
              - exe1: {}
                name: Run exe1
        """).lstrip()
