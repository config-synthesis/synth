"""Tests for ``synth.synthesis.knowledge_base``."""


# Imports.
from collections import namedtuple
from unittest.mock import Mock

import pytest
from sqlalchemy.sql import cast, select
from sqlalchemy.types import JSON

from synth.synthesis import knowledge_base as kb
from synth.synthesis.classes import (
    ConfigurationSystem,
    ConfigurationTask,
    ConfigurationTaskArgument,
    ConfigurationTaskArgumentMapping,
    FileAdd,
    FileDelete,
    ShellTaskError,
)
from synth.synthesis.knowledge_base import task_execution


# Test query results.
TaskErrorsProxy = namedtuple(
    'TaskErrorsProxy',
    ('id', 'task', 'error')
)


class TestInsertTaskExecution:
    """Tests for ``kb.insert_task_executions``."""

    @pytest.fixture(autouse=True)
    def exists(self, sqlalchemy_execute: Mock):
        """Mock exists for testing."""
        sqlalchemy_execute.return_value = iter(((True,),))

    @pytest.fixture
    def task(self) -> ConfigurationTask:
        """Create a configuration task for testing."""
        return ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='exe',
            arguments=(),
            changes=frozenset(),
        )

    @pytest.fixture
    def error(self) -> ShellTaskError:
        """Error for testing."""
        return ShellTaskError.from_primitives(
            exit_code=1,
            stdout='',
            stderr='error',
        )

    @pytest.fixture
    def resolving_tasks(self) -> tuple[ConfigurationTask, ...]:
        """Create resolving tasks for testing."""
        return (
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe1',
                arguments=(),
                changes=frozenset(),
            ),
            ConfigurationTask(
                system=ConfigurationSystem.SHELL,
                executable='exe2',
                arguments=(),
                changes=frozenset(),
            ),
        )

    def test_cast_task(self,
                       sqlalchemy_execute: Mock,
                       task: ConfigurationTask):
        """Verify the task is cast to JSON."""
        expected = select(
            select([
                task_execution.c.id,
            ])
            .where(
                task_execution.c.task == cast(task, JSON),
                task_execution.c.error == None,  # noqa: E711
                task_execution.c.resolving_tasks == None,  # noqa: E711
            )
            .exists()
        )

        kb.insert_task_executions([(None, task, None, None)])
        sqlalchemy_execute.assert_called_once()
        assert len(sqlalchemy_execute.call_args.args) == 1
        assert expected.compare(sqlalchemy_execute.call_args.args[0])

    def test_cast_error(self,
                        sqlalchemy_execute: Mock,
                        task: ConfigurationTask,
                        error: ShellTaskError):
        """Verify the error is cast to JSON."""
        expected = select(
            select([
                task_execution.c.id,
            ])
            .where(
                task_execution.c.task == cast(task, JSON),
                task_execution.c.error == cast(error, JSON),
                task_execution.c.resolving_tasks == None,  # noqa: E711
            )
            .exists()
        )

        kb.insert_task_executions([(None, task, error, None)])
        sqlalchemy_execute.assert_called_once()
        assert len(sqlalchemy_execute.call_args.args) == 1
        assert expected.compare(sqlalchemy_execute.call_args.args[0])

    def test_cast_no_error(self,
                           sqlalchemy_execute: Mock,
                           task: ConfigurationTask):
        """Verify None error is cast to SQL NULL."""
        expected = select(
            select([
                task_execution.c.id,
            ])
            .where(
                task_execution.c.task == cast(task, JSON),
                task_execution.c.error == None,  # noqa: E711
                task_execution.c.resolving_tasks == None,  # noqa: E711
            )
            .exists()
        )

        kb.insert_task_executions([(None, task, None, None)])
        sqlalchemy_execute.assert_called_once()
        assert len(sqlalchemy_execute.call_args.args) == 1
        assert expected.compare(sqlalchemy_execute.call_args.args[0])

    def test_cast_resolving_tasks(self,
                                  sqlalchemy_execute: Mock,
                                  task: ConfigurationTask,
                                  resolving_tasks:
                                  tuple[ConfigurationTask, ...]):
        """Verify the resolving tasks are cast to JSON."""
        expected = select(
            select([
                task_execution.c.id,
            ])
            .where(
                task_execution.c.task == cast(task, JSON),
                task_execution.c.error == None,  # noqa: E711
                task_execution.c.resolving_tasks == cast(
                    resolving_tasks,
                    JSON,
                ),
            )
            .exists()
        )

        kb.insert_task_executions([(None, task, None, resolving_tasks)])
        sqlalchemy_execute.assert_called_once()
        assert len(sqlalchemy_execute.call_args.args) == 1
        assert expected.compare(sqlalchemy_execute.call_args.args[0])

    def test_cast_no_resolving_tasks(self,
                                     sqlalchemy_execute: Mock,
                                     task: ConfigurationTask):
        """Verify None resolving tasks is cast to SQL NULL."""
        expected = select(
            select([
                task_execution.c.id,
            ])
            .where(
                task_execution.c.task == cast(task, JSON),
                task_execution.c.error == None,  # noqa: E711
                task_execution.c.resolving_tasks == None,  # noqa: E711
            )
            .exists()
        )

        kb.insert_task_executions([(None, task, None, None)])
        sqlalchemy_execute.assert_called_once()
        assert len(sqlalchemy_execute.call_args.args) == 1
        assert expected.compare(sqlalchemy_execute.call_args.args[0])

    def test_exists(self,
                    sqlalchemy_execute: Mock,
                    task: ConfigurationTask):
        """Verify no insert is performed if the record exists."""
        expected = select(
            select([
                task_execution.c.id,
            ])
            .where(
                task_execution.c.task == cast(task, JSON),
                task_execution.c.error == None,  # noqa: E711
                task_execution.c.resolving_tasks == None,  # noqa: E711
            )
            .exists()
        )

        kb.insert_task_executions([(None, task, None, None)])
        sqlalchemy_execute.assert_called_once()
        assert len(sqlalchemy_execute.call_args.args) == 1
        assert expected.compare(sqlalchemy_execute.call_args.args[0])

    def test_insert(self,
                    sqlalchemy_execute: Mock,
                    task: ConfigurationTask,
                    error: ShellTaskError,
                    resolving_tasks: tuple[ConfigurationTask, ...]):
        """Verify an insert is executed if a record does not exist."""
        sqlalchemy_execute.return_value = iter(((False,),))

        level = 9
        expected_query = select(
            select([
                task_execution.c.id,
            ])
            .where(
                task_execution.c.task == cast(task, JSON),
                task_execution.c.error == cast(error, JSON),
                task_execution.c.resolving_tasks == cast(
                    resolving_tasks,
                    JSON,
                ),
                task_execution.c.level == level,
            )
            .exists()
        )
        expected_insert = task_execution.insert({
            'system': task.system,
            'task': task,
            'error': error,
            'resolving_tasks': resolving_tasks,
            'level': level,
        })

        kb.insert_task_executions([(level, task, error, resolving_tasks)])

        assert sqlalchemy_execute.call_count == 2

        assert len(sqlalchemy_execute.call_args_list[0].args) == 1
        assert expected_query.compare(
            sqlalchemy_execute.call_args_list[0].args[0]
        )

        assert len(sqlalchemy_execute.call_args_list[1].args) == 1
        assert expected_insert.compare(
            sqlalchemy_execute.call_args_list[1].args[0]
        )


class TestGetResolvingTasks:
    """Tests for ``kb.get_resolving_tasks``."""

    @pytest.fixture
    def arg_1(self) -> ConfigurationTaskArgument:
        """Create a configuration task argument."""
        return ConfigurationTaskArgument(original_value='arg1')

    @pytest.fixture
    def arg_2(self) -> ConfigurationTaskArgument:
        """Create a configuration task argument."""
        return ConfigurationTaskArgument(original_value='arg2')

    @pytest.fixture
    def arg_3(self) -> ConfigurationTaskArgument:
        """Create a configuration task argument."""
        return ConfigurationTaskArgument(original_value='arg3')

    @pytest.fixture
    def empty_mapping(self) -> ConfigurationTaskArgumentMapping:
        """Create an empty argument mapping."""
        return ConfigurationTaskArgumentMapping()

    @pytest.fixture
    def map_a1_a2(self,
                  arg_1: ConfigurationTaskArgument,
                  arg_2: ConfigurationTaskArgument,
                  ) -> ConfigurationTaskArgumentMapping:
        """Map arg_1 to arg_2."""
        return ConfigurationTaskArgumentMapping([
            (arg_1, arg_2),
        ])

    @pytest.fixture
    def map_a2_a1(self,
                  arg_2: ConfigurationTaskArgument,
                  arg_1: ConfigurationTaskArgument,
                  ) -> ConfigurationTaskArgumentMapping:
        """Map arg_2 to arg_1."""
        return ConfigurationTaskArgumentMapping([
            (arg_2, arg_1),
        ])

    @pytest.fixture
    def map_a1_a3(self,
                  arg_1: ConfigurationTaskArgument,
                  arg_3: ConfigurationTaskArgument,
                  ) -> ConfigurationTaskArgumentMapping:
        """Map arg_1 to arg_3."""
        return ConfigurationTaskArgumentMapping([
            (arg_1, arg_3),
        ])

    @pytest.fixture
    def map_a3_a1(self,
                  arg_3: ConfigurationTaskArgument,
                  arg_1: ConfigurationTaskArgument,
                  ) -> ConfigurationTaskArgumentMapping:
        """Map arg_3 to arg_1."""
        return ConfigurationTaskArgumentMapping([
            (arg_3, arg_1),
        ])

    @pytest.fixture
    def map_a2_a3(self,
                  arg_2: ConfigurationTaskArgument,
                  arg_3: ConfigurationTaskArgument,
                  ) -> ConfigurationTaskArgumentMapping:
        """Map arg_2 to arg_3."""
        return ConfigurationTaskArgumentMapping([
            (arg_2, arg_3),
        ])

    @pytest.fixture
    def map_a3_a2(self,
                  arg_3: ConfigurationTaskArgument,
                  arg_2: ConfigurationTaskArgument,
                  ) -> ConfigurationTaskArgumentMapping:
        """Map arg_3 to arg_2."""
        return ConfigurationTaskArgumentMapping([
            (arg_3, arg_2),
        ])

    @pytest.fixture
    def rm_arg_1(self, arg_1: ConfigurationTaskArgument) -> ConfigurationTask:
        """Create a configuration task for ``rm <arg_1>``."""
        task = ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='rm',
            arguments=(arg_1.original_value,),
            changes=frozenset({
                FileDelete.from_primitives(path=arg_1.original_value),
            }),
        )
        return task

    @pytest.fixture
    def rm_arg_2(self, arg_2: ConfigurationTaskArgument) -> ConfigurationTask:
        """Create a configuration task for ``rm <arg_2>``."""
        task = ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='rm',
            arguments=(arg_2.original_value,),
            changes=frozenset({
                FileDelete.from_primitives(path=arg_2.original_value,),
            }),
        )
        return task

    @pytest.fixture
    def rm_arg_1_err(self, arg_1: ConfigurationTaskArgument) -> ShellTaskError:
        """Create an error for ``rm <arg_1>``."""
        return ShellTaskError.from_primitives(
            exit_code=1,
            stdout='',
            stderr=f'rm: {arg_1.original_value}: No such file or directory',
            arguments=frozenset({arg_1}),
        )

    @pytest.fixture
    def rm_arg_2_err(self, arg_2: ConfigurationTaskArgument) -> ShellTaskError:
        """Create an error for ``rm <arg_2>``."""
        return ShellTaskError.from_primitives(
            exit_code=1,
            stdout='',
            stderr=f'rm: {arg_2.original_value}: No such file or directory',
            arguments=frozenset({arg_2}),
        )

    @pytest.fixture
    def rm_arg_3_err(self, arg_3: ConfigurationTaskArgument) -> ShellTaskError:
        """Create an error for ``rm <arg_3>``."""
        return ShellTaskError.from_primitives(
            exit_code=1,
            stdout='',
            stderr=f'rm: {arg_3.original_value}: No such file or directory',
            arguments=frozenset({arg_3}),
        )

    @pytest.fixture
    def rm_arg_1_resolving(self,
                           arg_1: ConfigurationTaskArgument
                           ) -> ConfigurationTask:
        """Create a resolving task for ``rm <arg_1>`` errors."""
        return ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='touch',
            arguments=(arg_1.original_value,),
            changes=frozenset({
                FileAdd.from_primitives(
                    path=arg_1.original_value,
                ),
            }),
        )

    @pytest.fixture
    def rm_arg_2_resolving(self,
                           arg_2: ConfigurationTaskArgument
                           ) -> ConfigurationTask:
        """Create a resolving task for ``rm <arg_2>`` errors."""
        return ConfigurationTask(
            system=ConfigurationSystem.SHELL,
            executable='touch',
            arguments=(arg_2.original_value,),
            changes=frozenset({
                FileAdd.from_primitives(
                    path=arg_2.original_value,
                ),
            }),
        )

    def test_task_exact_match_without_mapping(
            self,
            rm_arg_1: ConfigurationTask,
            empty_mapping: ConfigurationTaskArgumentMapping,
            rm_arg_1_err: ShellTaskError,
            rm_arg_1_resolving: ConfigurationTask,
            sqlalchemy_execute: Mock):
        """Verify an exact task match returns the correct resolving tasks.

        The input mapping for the task should be empty.

        Task:
            rm <arg_1>
        Error:
            rm: <arg_1>: No such file or directory
        Matching Task:
            rm <arg_1>
        Resolving Task:
            touch <arg_1>
        """
        rm_task_id = 1
        mock_tasks_result = Mock()
        mock_tasks_result.yield_per.return_value = [
            TaskErrorsProxy(
                rm_task_id,
                rm_arg_1,
                rm_arg_1_err,
            ),
        ]
        mock_resolving_tasks_result = Mock()
        mock_resolving_tasks_result.one.return_value.resolving_tasks = [
            rm_arg_1_resolving
        ]
        sqlalchemy_execute.side_effect = [
            mock_tasks_result,
            mock_resolving_tasks_result,
        ]

        resolving_tasks = kb.get_resolving_tasks(rm_arg_1, rm_arg_1_err)

        assert resolving_tasks == [
            (rm_arg_1_resolving.no_changes(), empty_mapping),
        ]

    def test_task_exact_match_with_mapping(
            self,
            rm_arg_1: ConfigurationTask,
            map_a1_a2: ConfigurationTaskArgumentMapping,
            rm_arg_2_err: ShellTaskError,
            rm_arg_2: ConfigurationTask,
            rm_arg_2_resolving: ConfigurationTask,
            empty_mapping: ConfigurationTaskArgumentMapping,
            sqlalchemy_execute: Mock):
        """Verify an exact task match returns the correct resolving tasks.

        The input mapping for the task should be non-empty.

        Task:
            rm <arg_1>
                <arg_1> => <arg_2>
        Error:
            rm: <arg_2>: No such file or directory
        Matching Task:
            rm <arg_2>
        Resolving:
            touch <arg_2>
        """
        rm_task_id = 2
        mock_tasks_result = Mock()
        mock_tasks_result.yield_per.return_value = [
            TaskErrorsProxy(
                rm_task_id,
                rm_arg_2,
                rm_arg_2_err,
            ),
        ]
        mock_resolving_tasks_result = Mock()
        mock_resolving_tasks_result.one.return_value.resolving_tasks = [
            rm_arg_2_resolving,
        ]
        sqlalchemy_execute.side_effect = [
            mock_tasks_result,
            mock_resolving_tasks_result,
        ]

        mapped_task = rm_arg_1.from_mapping(map_a1_a2)
        resolving_tasks = kb.get_resolving_tasks(mapped_task, rm_arg_2_err)

        assert resolving_tasks == [
            (rm_arg_2_resolving.no_changes(), empty_mapping)
        ]

    def test_task_maps_without_mapping(
            self,
            rm_arg_1: ConfigurationTask,
            empty_mapping: ConfigurationTaskArgumentMapping,
            rm_arg_1_err: ShellTaskError,
            rm_arg_2: ConfigurationTask,
            rm_arg_2_err: ShellTaskError,
            rm_arg_2_resolving: ConfigurationTask,
            map_a2_a1: ConfigurationTaskArgumentMapping,
            sqlalchemy_execute: Mock):
        """Verify a mapped task match returns the correct resolving tasks.

        Task:
            rm <arg_1>
        Error:
            rm: <arg_1>: No such file or directory
        Matching Task:
            rm <arg_2>
                <arg_1> => <arg_2>
        Resolving:
            touch <arg_2>
                <arg_2> => <arg_1>
        """
        rm_task_id = 2
        mock_tasks_result = Mock()
        mock_tasks_result.yield_per.return_value = [
            TaskErrorsProxy(
                rm_task_id,
                rm_arg_2,
                rm_arg_2_err,
            ),
        ]
        mock_resolving_tasks_result = Mock()
        mock_resolving_tasks_result.one.return_value.resolving_tasks = [
            rm_arg_2_resolving,
        ]
        sqlalchemy_execute.side_effect = [
            mock_tasks_result,
            mock_resolving_tasks_result,
        ]

        resolving_tasks = kb.get_resolving_tasks(rm_arg_1, rm_arg_1_err)

        assert resolving_tasks == [
            (rm_arg_2_resolving.no_changes(), map_a2_a1),
        ]

    def test_task_maps_with_mapping(
            self,
            rm_arg_1: ConfigurationTask,
            map_a1_a3: ConfigurationTaskArgumentMapping,
            rm_arg_3_err: ShellTaskError,
            rm_arg_2: ConfigurationTask,
            rm_arg_2_err: ShellTaskError,
            rm_arg_2_resolving: ConfigurationTask,
            map_a2_a3: ConfigurationTaskArgumentMapping,
            sqlalchemy_execute: Mock):
        """Verify a mapped task match returns the correct resolving tasks.

        Task:
            rm <arg_1>
                <arg_1> => <arg_3>
        Error:
            rm: <arg_3>: No such file or directory
        Matching Task:
            rm <arg_2>
                <arg_3> => <arg_2>
        Resolving:
            touch <arg_2>
                <arg_2> => <arg_3>
        """
        rm_task_id = 2
        mock_tasks_result = Mock()
        mock_tasks_result.yield_per.return_value = [
            TaskErrorsProxy(
                rm_task_id,
                rm_arg_2,
                rm_arg_2_err,
            ),
        ]
        mock_resolving_tasks_result = Mock()
        mock_resolving_tasks_result.one.return_value.resolving_tasks = [
            rm_arg_2_resolving,
        ]
        sqlalchemy_execute.side_effect = [
            mock_tasks_result,
            mock_resolving_tasks_result,
        ]

        mapped_task = rm_arg_1.from_mapping(map_a1_a3)
        resolving_tasks = kb.get_resolving_tasks(mapped_task, rm_arg_3_err)

        assert resolving_tasks == [
            (rm_arg_2_resolving.no_changes(), map_a2_a3),
        ]
