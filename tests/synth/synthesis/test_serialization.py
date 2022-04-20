"""Tests for ``synth.synthesis.serialization``."""


# Imports.
import json

import pytest

from synth.synthesis.classes import (
    ConfigurationSystem,
    ConfigurationTask,
    FileAdd,
    FileChange,
    FileContentChange,
    FileContentChangeType,
    FileDelete,
    frozendict,
    SyntheticValue,
)
from synth.synthesis.serialization import from_dict, SynthJSONEncoder
from tests.test_utils import OrderedSet


@pytest.fixture
def encoder() -> SynthJSONEncoder:
    """Create an encoder for testing."""
    return SynthJSONEncoder()


class TestFromDict:
    """Tests for ``from_dict``."""

    def test_returns_regular_dicts(self):
        """Verify regular dicts are returned unaltered."""
        val = {'key': 'value'}

        assert from_dict(val) == val

    def test_sets_correct_types(self):
        """Verify the correct types are reconstructed."""
        # from_dict is called bottom up by the JSON load process and only needs
        # to handle one level, so changes are not included in this test.
        val = {
            'type': FileChange.__name__,
            'value': {
                'path': 'file.txt',
                'changes': [],
            }
        }

        o = from_dict(val)

        assert isinstance(o, FileChange)
        assert isinstance(o.path, SyntheticValue)
        assert isinstance(o.changes, tuple)
        assert len(o.changes) == 0


class TestSynthJSONEncoder:
    """Tests for ``SynthJSONEncoder``."""

    class TestInit:
        """Tests for ``DataclassJSONEncoder.__init__``."""

        def test_sets_sort_keys(self):
            """Verify ``sort_keys`` is always set to true."""
            encoder = SynthJSONEncoder(sort_keys=False)

            assert encoder.sort_keys is True

    class TestDefault:
        """Tests for ``DataclassJSONEncoder.default``."""

        def test_encodes_synthetic_values(self, encoder: SynthJSONEncoder):
            """Verify synthetic values are encoded as their original value."""
            sv = SyntheticValue(original_value='1234', arguments=frozenset())

            assert encoder.default(sv) == sv.original_value

        def test_encodes_dataclasses(self, encoder: SynthJSONEncoder):
            """Verify dataclasses are encoded."""
            change = FileAdd.from_primitives(path='file.txt')

            assert encoder.default(change) == {
                'type': FileAdd.__name__,
                'value': change.__dict__,
            }

        def test_encodes_enums(self, encoder: SynthJSONEncoder):
            """Verify enums are encoded as their value."""
            enum = FileContentChangeType.ADDITION

            assert encoder.default(enum) == enum.value

        def test_encodes_sets(self, encoder: SynthJSONEncoder):
            """Verify sets are encoded as sorted lists."""
            s = OrderedSet([3, 2, 1])

            assert encoder.default(s) == [1, 2, 3]

        def test_encodes_mappings(self, encoder: SynthJSONEncoder):
            """Verify mappings are encoded as dicts."""
            d = {
                'a': 1,
                'b': 2,
                'c': 3,
            }
            mapping = frozendict(d)

            encoded = encoder.default(mapping)

            assert type(encoded) == dict
            assert encoded == d

        def test_preserves_non_dataclasses(self,
                                           encoder: SynthJSONEncoder):
            """Verify non-dataclasses are returned as-is."""
            assert encoder.default(1) == 1
            assert encoder.default('a') == 'a'
            assert encoder.default(o := object()) == o
            assert encoder.default({}) == {}

    class TestEncode:
        """Tests for the full ``DataclassJSONEncoder.encode`` process."""

        def test_encodes_tree(self, encoder: SynthJSONEncoder):
            """Verify an entire tree is encoded correctly."""
            change = FileChange.from_primitives(
                path='file.txt',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='content',
                    ),
                ),
            )

            json_dict = json.loads(encoder.encode(change))

            assert json_dict == {
                'type': FileChange.__name__,
                'value': {
                    'path': change.path.original_value,
                    'changes': [
                        {
                            'type': FileContentChange.__name__,
                            'value': {
                                'change_type': 'addition',
                                'content': 'content',
                            },
                        },
                    ],
                }
            }

        def test_sorts_keys(self, encoder: SynthJSONEncoder):
            """Verify dict keys are sorted."""
            d = {
                'c': 3,
                'b': 2,
                'a': 1,
            }

            json_dict = json.loads(encoder.encode(d))

            assert list(json_dict.keys()) == ['a', 'b', 'c']


class TestJSONRoundTrip:
    """Verify the whole JSON serialize/deserialize process."""

    def test_configuration_change(self, encoder: SynthJSONEncoder):
        """Verify a configuration change can be serialized/deserialized."""
        change = FileChange.from_primitives(
            path='file.txt',
            changes=(
                FileContentChange.from_primitives(
                    change_type=FileContentChangeType.ADDITION,
                    content='content',
                ),
            ),
        )

        result = json.loads(encoder.encode(change), object_hook=from_dict)

        assert result == change

    def test_shell_task(self, encoder: SynthJSONEncoder):
        """Verify a shell configuration task can be serialized/deserialized."""
        task = ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='touch',
            arguments=('file.txt',),
            changes=frozenset({
                FileAdd.from_primitives(
                    path='file.txt',
                ),
            }),
        )

        result = json.loads(encoder.encode(task), object_hook=from_dict)

        assert result == task

    def test_ansible_task(self, encoder: SynthJSONEncoder):
        """Verify an ansible task can be serialized/deserialized."""
        task = ConfigurationTask(
            system=ConfigurationSystem.ANSIBLE,
            executable='lineinfile',
            arguments=frozendict({'path': 'file.txt', 'line': 'content'}),
            changes=frozenset({
                FileChange.from_primitives(
                    path='file.txt',
                    changes=(
                        FileContentChange.from_primitives(
                            change_type=FileContentChangeType.ADDITION,
                            content='content'
                        ),
                    ),
                ),
            }),
        )

        result = json.loads(encoder.encode(task), object_hook=from_dict)

        assert result == task

    def test_sorts(self, encoder: SynthJSONEncoder):
        """Verify encoded JSON objects are sorted correctly."""
        task = ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='exe',
            arguments=('c', 'b', 'a'),
            changes=frozenset({
                FileChange.from_primitives(
                    path='b',
                    changes=(
                        FileContentChange.from_primitives(
                            change_type=FileContentChangeType.DELETION,
                            content='1',
                        ),
                        FileContentChange.from_primitives(
                            change_type=FileContentChangeType.ADDITION,
                            content='1',
                        ),
                    ),
                ),
                FileChange.from_primitives(
                    path='a',
                    changes=(
                        FileContentChange.from_primitives(
                            change_type=FileContentChangeType.ADDITION,
                            content='1',
                        ),
                        FileContentChange.from_primitives(
                            change_type=FileContentChangeType.DELETION,
                            content='3',
                        ),
                    ),
                ),
                FileChange.from_primitives(
                    path='a',
                    changes=(
                        FileContentChange.from_primitives(
                            change_type=FileContentChangeType.DELETION,
                            content='1',
                        ),
                        FileContentChange.from_primitives(
                            change_type=FileContentChangeType.ADDITION,
                            content='2',
                        ),
                    ),
                ),
                FileDelete.from_primitives(
                    path='path-a',
                ),
                FileAdd.from_primitives(
                    path='path-b',
                ),
                FileAdd.from_primitives(
                    path='path-a',
                ),
            }),
        )

        encoded = encoder.encode(task)

        decoded_task = json.loads(encoded, object_hook=from_dict)
        assert decoded_task == task

        addition = FileContentChangeType.ADDITION.value
        deletion = FileContentChangeType.DELETION.value
        decoded_json = json.loads(encoded)
        assert decoded_json['value']['arguments'] == ['c', 'b', 'a']
        assert decoded_json['value']['changes'] == [
            {
                'type': FileAdd.__name__,
                'value': {'path': 'path-a'},
            },
            {
                'type': FileAdd.__name__,
                'value': {'path': 'path-b'},
            },
            {
                'type': FileChange.__name__,
                'value': {
                    'path': 'a',
                    'changes': [
                        {
                            'type': FileContentChange.__name__,
                            'value': {
                                'change_type': addition,
                                'content': '1',
                            },
                        },
                        {
                            'type': FileContentChange.__name__,
                            'value': {
                                'change_type': deletion,
                                'content': '3',
                            },
                        }
                    ],
                },
            },
            {
                'type': FileChange.__name__,
                'value': {
                    'path': 'a',
                    'changes': [
                        {
                            'type': FileContentChange.__name__,
                            'value': {
                                'change_type': deletion,
                                'content': '1',
                            },
                        },
                        {
                            'type': FileContentChange.__name__,
                            'value': {
                                'change_type': addition,
                                'content': '2',
                            },
                        }
                    ],
                },
            },
            {
                'type': FileChange.__name__,
                'value': {
                    'path': 'b',
                    'changes': [
                        {
                            'type': FileContentChange.__name__,
                            'value': {
                                'change_type': deletion,
                                'content': '1',
                            },
                        },
                        {
                            'type': FileContentChange.__name__,
                            'value': {
                                'change_type': addition,
                                'content': '1',
                            },
                        }
                    ],
                },
            },
            {
                'type': FileDelete.__name__,
                'value': {'path': 'path-a'},
            },
        ]
