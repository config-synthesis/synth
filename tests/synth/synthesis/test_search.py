"""Tests for synthesis search."""


# Imports.
from collections import namedtuple
from collections.abc import Generator
from unittest.mock import call, Mock, patch

import pytest
from docker import client

from synth.synthesis import knowledge_base as kb, search
from synth.synthesis.classes import (
    ConfigurationChange,
    ConfigurationSystem,
    ConfigurationTask,
    ConfigurationTaskArgument,
    ConfigurationTaskArgumentMapping,
    FileAdd,
    FileChange,
    FileContentChange,
    FileContentChangeType,
    FileDelete,
    ShellTaskError,
)
from synth.synthesis.docker import RunResult
from synth.synthesis.exceptions import UnresolvedTaskFailure
from synth.synthesis.search import (
    get_task_ordering,
    get_task_set,
    SearchResult,
)
from tests.test_utils import OrderedSet


# Test query results.
TaskErrorsProxy = namedtuple(
    'TaskErrorsProxy',
    ('id', 'task', 'error')
)


class TestGetTaskSet:
    """Tests for ``get_task_set``."""

    @pytest.fixture
    def madlibs(self) -> ConfigurationTask:
        """Create a task for testing."""
        task = ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='madlibs',
            arguments=('fox', 'dog'),
            changes=frozenset({
                FileAdd.from_primitives(path='1.txt'),
                FileChange.from_primitives(
                    path='1.txt',
                    changes=(
                        FileContentChange.from_primitives(
                            change_type=FileContentChangeType.ADDITION,
                            content='+fox+dog+',
                        ),
                    ),
                ),
                FileAdd.from_primitives(path='2.txt'),
                FileChange.from_primitives(
                    path='2.txt',
                    changes=(
                        FileContentChange.from_primitives(
                            change_type=FileContentChangeType.ADDITION,
                            content='+dog+fox+',
                        ),
                    ),
                ),
            }),
        )
        return task

    @pytest.fixture(autouse=True)
    def kb(self, madlibs: ConfigurationTask) -> Generator[Mock, None, None]:
        """Mock the knowledge base for testing."""
        with patch.object(kb, 'get_configuration_tasks') as mock:
            mock.return_value = {madlibs}
            yield mock

    @pytest.fixture
    def change_set_a(self) -> set[ConfigurationChange]:
        """Create a change set for testing."""
        return {
            FileAdd.from_primitives(path='1.txt'),
            FileChange.from_primitives(
                path='1.txt',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='+vulpine+++canine+',
                    ),
                ),
            ),
        }

    @pytest.fixture
    def change_set_b(self) -> set[ConfigurationChange]:
        """Create a change set for testing."""
        return {
            FileAdd.from_primitives(path='2.txt'),
            FileChange.from_primitives(
                path='2.txt',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='++canine+vulpine++',
                    ),
                ),
            ),
        }

    @pytest.fixture
    def change_set_c(self) -> set[ConfigurationChange]:
        """Create a change set for testing."""
        return {
            FileAdd.from_primitives(path='1.txt'),
            FileChange.from_primitives(
                path='1.txt',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='+vulpine+++feline+',
                    ),
                ),
            ),
        }

    @pytest.fixture
    def change_set_d(self) -> set[ConfigurationChange]:
        """Create a change set for testing."""
        return {
            FileAdd.from_primitives(path='2.txt'),
            FileChange.from_primitives(
                path='2.txt',
                changes=(
                    FileContentChange.from_primitives(
                        change_type=FileContentChangeType.ADDITION,
                        content='++feline+vulpine++',
                    ),
                ),
            ),
        }

    def test_exact_cover(self,
                         madlibs: ConfigurationTask,
                         change_set_a: set[ConfigurationChange],
                         change_set_b: set[ConfigurationChange]):
        """Verify an exact covering is returned."""
        a_fox = ConfigurationTaskArgument(original_value='fox')
        a_dog = ConfigurationTaskArgument(original_value='dog')
        a_vulpine = ConfigurationTaskArgument(original_value='vulpine+')
        a_canine = ConfigurationTaskArgument(original_value='+canine')

        changes = change_set_a | change_set_b
        task_set = get_task_set(changes, ConfigurationSystem.SHELL)

        assert len(task_set) == 1

        result = task_set.pop()
        assert result.original_task == madlibs
        assert result.mapping in {
            ConfigurationTaskArgumentMapping([
                (a_fox, a_vulpine),
                (a_dog, a_canine),
            ]),
            ConfigurationTaskArgumentMapping([
                (a_fox, a_canine),
                (a_dog, a_vulpine),
            ]),
        }
        assert result.task == result.original_task.from_mapping(result.mapping)

    def test_extra_cover(self,
                         madlibs: ConfigurationTask,
                         change_set_a: set[ConfigurationChange]):
        """Verify a covering with extra changes is returned."""
        a_fox = ConfigurationTaskArgument(original_value='fox')
        a_dog = ConfigurationTaskArgument(original_value='dog')
        a_vulpine = ConfigurationTaskArgument(original_value='vulpine')
        a_vulpine_p = ConfigurationTaskArgument(original_value='vulpine+')
        a_vulpine_pp = ConfigurationTaskArgument(original_value='vulpine++')
        a_canine = ConfigurationTaskArgument(original_value='canine')
        a_canine_p = ConfigurationTaskArgument(original_value='+canine')
        a_canine_pp = ConfigurationTaskArgument(original_value='++canine')

        task_set = get_task_set(change_set_a, ConfigurationSystem.SHELL)

        assert len(task_set) == 1

        result = task_set.pop()
        assert result.original_task == madlibs
        assert result.mapping in {
            ConfigurationTaskArgumentMapping([
                (a_fox, a_vulpine),
                (a_dog, a_canine_pp),
            ]),
            ConfigurationTaskArgumentMapping([
                (a_fox, a_vulpine_p),
                (a_dog, a_canine_p),
            ]),
            ConfigurationTaskArgumentMapping([
                (a_fox, a_vulpine_pp),
                (a_dog, a_canine),
            ]),
        }
        assert result.task == result.original_task.from_mapping(result.mapping)

    def test_single_task_used_twice(self,
                                    madlibs: ConfigurationTask,
                                    change_set_a: set[ConfigurationChange],
                                    change_set_b: set[ConfigurationChange],
                                    change_set_c: set[ConfigurationChange],
                                    change_set_d: set[ConfigurationChange]):
        """Verify a single task can be used twice to cover changes."""
        a_fox = ConfigurationTaskArgument(original_value='fox')
        a_dog = ConfigurationTaskArgument(original_value='dog')
        a_vulpine = ConfigurationTaskArgument(original_value='vulpine+')
        a_canine = ConfigurationTaskArgument(original_value='+canine')
        a_feline = ConfigurationTaskArgument(original_value='+feline')

        changes = change_set_a | change_set_b | change_set_c | change_set_d
        task_set = get_task_set(changes, ConfigurationSystem.SHELL)

        assert len(task_set) == 2

        for result in task_set:
            assert result.original_task == madlibs
            assert result.mapping in {
                ConfigurationTaskArgumentMapping([
                    (a_fox, a_vulpine),
                    (a_dog, a_canine),
                ]),
                ConfigurationTaskArgumentMapping([
                    (a_fox, a_canine),
                    (a_dog, a_vulpine),
                ]),
                ConfigurationTaskArgumentMapping([
                    (a_fox, a_vulpine),
                    (a_dog, a_feline),
                ]),
                ConfigurationTaskArgumentMapping([
                    (a_fox, a_feline),
                    (a_dog, a_vulpine),
                ]),
            }
            assert result.task == result.original_task.from_mapping(
                result.mapping
            )


class TestGetTaskOrdering:
    """Tests for ``search.get_task_ordering``."""

    @pytest.fixture(autouse=True)
    def from_env(self) -> Generator[Mock, None, None]:
        """Patch the Docker client creation."""
        with patch.object(client, 'from_env') as mock:
            yield mock

    @pytest.fixture(autouse=True)
    def runner(self) -> Generator[Mock, None, None]:
        """Patch ``run_task`` for testing."""
        with patch.object(search, 'get_runner') as mock:
            # The search process uses the runner returned by
            # get_runner().__enter__(). Note that the mock __enter__ isn't
            # configured to return the same object (like the actual
            # implementation) so that tests can verify calls without also
            # checking for __enter__ and __exit__.
            yield mock().__enter__()

    def test_empty_set(self):
        """Verify the empty set returns the empty list."""
        assert get_task_ordering(set()) == []

    def test_no_errors(self, runner: Mock):
        """Verify that the initial ordering is returned without errors."""
        changes = frozenset({
            FileAdd.from_primitives(path='file.txt'),
        })
        results = [
            SearchResult(
                task=ConfigurationTask(
                    system=ConfigurationSystem.SHELL,
                    executable='touch',
                    arguments=('file.txt',),
                    changes=changes,
                ),
            ),
        ]

        runner.run_task.side_effect = [
            RunResult(0, '', ''),
        ]

        ordering = get_task_ordering(OrderedSet(results))

        assert ordering == results
        runner.run_task.assert_called_once_with(
            task=results[0].task,
            arguments=results[0].task.configuration_task_arguments,
        )

    def test_errors_no_matching_future_tasks(self,
                                             runner: Mock,
                                             sqlalchemy_execute: Mock):
        """Verify resolving tasks are correctly inserted with errors."""
        mkdir = ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='mkdir',
            arguments=('dir',),
            changes=frozenset(),  # No directory change type yet.
        )
        touch = ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='touch',
            arguments=('dir/file.txt',),
            changes=frozenset({
                FileAdd.from_primitives(
                    path='dir/file.txt',
                ),
            })
        )
        rm = ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='rm',
            arguments=('dir/file.txt',),
            changes=frozenset({
                FileDelete.from_primitives(path='dir/file.txt'),
            }),
        )
        rm_err = ShellTaskError.from_primitives(
            exit_code=1,
            stdout='',
            stderr='rm: dir: No such file or directory',
        )
        empty_mapping = ConfigurationTaskArgumentMapping()

        rm_task_id = 1
        mock_tasks_result = Mock()
        mock_tasks_result.yield_per.return_value = [
            TaskErrorsProxy(rm_task_id, rm, rm_err),
        ]
        mock_resolving_tasks_result = Mock()
        mock_resolving_tasks_result.one.return_value.resolving_tasks = (
            mkdir,
            touch,
        )
        sqlalchemy_execute.side_effect = [
            mock_tasks_result,
            mock_resolving_tasks_result,
        ]

        runner.run_task.side_effect = [
            # First `rm` run.
            rm_err,

            # `mkdir`, `touch`, and second `rm` runs.
            RunResult(0, '', ''),
            RunResult(0, '', ''),
            RunResult(0, '', ''),
        ]

        ordering = get_task_ordering(OrderedSet([
            SearchResult(
                task=rm,
                original_task=rm,
                mapping=empty_mapping,
            ),
        ]))

        # Covering changes are not expected for mkdir and touch because they
        # are not a part of the original search result and weren't selected
        # because they contributed to the covering.
        assert ordering == [
            SearchResult(
                task=mkdir.no_changes(),
                original_task=mkdir.no_changes(),
                mapping=empty_mapping,
            ),
            SearchResult(
                task=touch.no_changes(),
                original_task=touch.no_changes(),
                mapping=empty_mapping,
            ),
            SearchResult(
                task=rm,
                original_task=rm,
                mapping=empty_mapping,
            ),
        ]
        runner.run_task.assert_has_calls([
            call(task=rm, arguments=rm.configuration_task_arguments),
            call(
                task=mkdir.no_changes(),
                arguments=mkdir.configuration_task_arguments,
            ),
            call(
                task=touch.no_changes(),
                arguments=touch.configuration_task_arguments,
            ),
            call(task=rm, arguments=rm.configuration_task_arguments),
        ])

    def test_errors_matching_future_tasks(self, runner: Mock,
                                          sqlalchemy_execute: Mock):
        """Verify tasks are pulled forward with resolving tasks."""
        mkdir = ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='mkdir',
            arguments=('dir',),
            changes=frozenset(),  # No directory change type yet.
        )
        touch = ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='touch',
            arguments=('dir/file.txt',),
            changes=frozenset({
                FileAdd.from_primitives(
                    path='dir/file.txt',
                ),
            })
        )
        rm = ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='rm',
            arguments=('dir/file.txt',),
            changes=frozenset({
                FileDelete.from_primitives(path='dir/file.txt'),
            }),
        )
        rm_err = ShellTaskError.from_primitives(
            exit_code=1,
            stdout='',
            stderr='rm: dir: No such file or directory',
        )
        empty_mapping = ConfigurationTaskArgumentMapping()

        rm_task_id = 1
        mock_tasks_result = Mock()
        mock_tasks_result.yield_per.return_value = [
            TaskErrorsProxy(rm_task_id, rm, rm_err),
        ]
        mock_resolving_tasks_result = Mock()
        mock_resolving_tasks_result.one.return_value.resolving_tasks = (
            mkdir,
            touch,
        )
        sqlalchemy_execute.side_effect = [
            mock_tasks_result,
            mock_resolving_tasks_result,
            mock_tasks_result,
            mock_resolving_tasks_result,
        ]

        runner.run_task.side_effect = [
            # First `rm` run.
            rm_err,

            # `mkdir`, `touch`, and second `rm` runs.
            RunResult(0, '', ''),
            RunResult(0, '', ''),
            RunResult(0, '', ''),
        ]

        ordering = get_task_ordering(OrderedSet([
            SearchResult(
                task=rm,
                original_task=rm,
                mapping=empty_mapping,
            ),
            SearchResult(
                task=touch,
                original_task=touch,
                mapping=empty_mapping,
            ),
        ]))

        assert ordering == [
            SearchResult(
                task=mkdir.no_changes(),
                original_task=mkdir.no_changes(),
                mapping=empty_mapping,
            ),
            SearchResult(
                task=touch,
                original_task=touch,
                mapping=empty_mapping,
            ),
            SearchResult(
                task=rm,
                original_task=rm,
                mapping=empty_mapping,
            ),
        ]
        runner.run_task.assert_has_calls([
            call(task=rm, arguments=rm.configuration_task_arguments),
            call(task=mkdir, arguments=mkdir.configuration_task_arguments),
            call(task=touch, arguments=touch.configuration_task_arguments),
            call(task=rm, arguments=rm.configuration_task_arguments),
        ])

    def test_raises_exception_on_unresolved_error(self, runner: Mock):
        """Verify an exception is raised when an error cannot be resolved."""
        changes = frozenset({
            FileAdd.from_primitives(path='file.txt'),
        })
        results = [
            SearchResult(
                task=ConfigurationTask(
                    system=ConfigurationSystem.SHELL,
                    executable='touch',
                    arguments=('file.txt',),
                    changes=changes,
                ),
            ),
        ]

        runner.run_task.side_effect = ShellTaskError.from_primitives(
            exit_code=0,
            stdout='',
            stderr='',
        )

        with pytest.raises(UnresolvedTaskFailure):
            get_task_ordering(OrderedSet(results), raise_on_unresolved=True)

        c = call(
            task=results[0].task,
            arguments=results[0].task.configuration_task_arguments,
        )
        runner.run_task.assert_has_calls((c, c))

    def test_drops_on_unresolved_error(self, runner: Mock):
        """Verify a task is dropped when an error cannot be resolved."""
        changes = frozenset({
            FileAdd.from_primitives(path='file.txt'),
        })
        results = [
            SearchResult(
                task=ConfigurationTask(
                    system=ConfigurationSystem.SHELL,
                    executable='touch',
                    arguments=('file.txt',),
                    changes=changes,
                ),
            ),
        ]

        runner.run_task.side_effect = ShellTaskError.from_primitives(
            exit_code=0,
            stdout='',
            stderr='',
        )

        assert get_task_ordering(OrderedSet(results)) == []
        c = call(
            task=results[0].task,
            arguments=results[0].task.configuration_task_arguments,
        )
        runner.run_task.assert_has_calls((c, c))

    def test_task_moved_to_end(self,
                               runner: Mock,
                               sqlalchemy_execute: Mock):
        """Verify the failed task is moved to the end."""
        tasks = [
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable=f'{i}',
                arguments=(),
                changes=frozenset(),
            )
            for i in range(6)
        ]
        t2_err = ShellTaskError.from_primitives(
            exit_code=1,
            stdout='',
            stderr='t2',
        )
        empty_mapping = ConfigurationTaskArgumentMapping()

        t2_task_id = 2
        mock_tasks_result = Mock()
        mock_tasks_result.yield_per.return_value = [
            TaskErrorsProxy(t2_task_id, tasks[2], t2_err),
        ]

        mock_resolving_tasks_result = Mock()
        mock_resolving_tasks_result.one.return_value.resolving_tasks = (
            tasks[5],
            tasks[4],
        )
        sqlalchemy_execute.side_effect = [
            mock_tasks_result,
            mock_resolving_tasks_result,
        ]

        runner.run_task.side_effect = [
            # t0 run
            RunResult(0, '', ''),

            # t1 run
            RunResult(0, '', ''),

            # First t2 run.
            t2_err,

            # t5 and t4 resolving task runs.
            RunResult(0, '', ''),
            RunResult(0, '', ''),

            # t3 run.
            RunResult(0, '', ''),

            # Final t2 run.
            RunResult(0, '', ''),
        ]

        ordering = get_task_ordering(OrderedSet([
            SearchResult(
                task=task,
                original_task=task,
                mapping=empty_mapping,
            )
            # Only use 0-4. 5 is for testing resolving tasks that aren't
            # originally present in the task set.
            for task in tasks[:5]
        ]))

        assert ordering == [
            SearchResult(
                task=tasks[0],
                original_task=tasks[0],
                mapping=empty_mapping,
            ),
            SearchResult(
                task=tasks[1],
                original_task=tasks[1],
                mapping=empty_mapping,
            ),
            SearchResult(
                task=tasks[5],
                original_task=tasks[5],
                mapping=empty_mapping,
            ),
            SearchResult(
                task=tasks[4],
                original_task=tasks[4],
                mapping=empty_mapping,
            ),
            SearchResult(
                task=tasks[3],
                original_task=tasks[3],
                mapping=empty_mapping,
            ),
            SearchResult(
                task=tasks[2],
                original_task=tasks[2],
                mapping=empty_mapping,
            ),
        ]
        runner.run_task.assert_has_calls([
            call(task=tasks[0], arguments=frozenset()),
            call(task=tasks[1], arguments=frozenset()),
            call(task=tasks[2], arguments=frozenset()),
            call(task=tasks[5], arguments=frozenset()),
            call(task=tasks[4], arguments=frozenset()),
            call(task=tasks[3], arguments=frozenset()),
            call(task=tasks[2], arguments=frozenset()),
        ])
