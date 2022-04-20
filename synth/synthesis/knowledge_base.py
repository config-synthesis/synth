"""The synthesis knowledge base."""


# Imports.
import json
from collections.abc import Iterable
from functools import wraps
from itertools import chain, zip_longest
from typing import Any, Callable, Optional, Union

import sqlalchemy
from sqlalchemy.engine import Connection
from sqlalchemy.exc import OperationalError
from sqlalchemy.schema import Column, MetaData, Table
from sqlalchemy.sql import cast, distinct, select
from sqlalchemy.sql.elements import Cast
from sqlalchemy.types import INTEGER, JSON, VARCHAR

from synth.logging import logger
from synth.settings import MYSQL_PASSWORD, MYSQL_USERNAME
from synth.synthesis.classes import (
    ConfigurationSystem,
    ConfigurationTask,
    ConfigurationTaskArgumentMapping,
    ConfigurationTaskError,
)
from synth.synthesis.serialization import from_dict, SynthJSONEncoder


# Types
ResolvingTaskTuple = tuple[Optional[int],
                           ConfigurationTask,
                           Optional[ConfigurationTaskError],
                           Optional[tuple[ConfigurationTask, ...]]]


# Custom Synth JSON encoder.
encoder = SynthJSONEncoder()

# Create a sqlalchemy engine, metadata, and tables.
if MYSQL_PASSWORD:
    user_str = f'{MYSQL_USERNAME}:{MYSQL_PASSWORD}'
else:
    user_str = MYSQL_USERNAME

mysql_engine = sqlalchemy.create_engine(
    f'mysql+pymysql://{user_str}@127.0.0.1:3306/synth?charset=utf8mb4'
    f'&binary_prefix=true',
    pool_recycle=3600,
    json_serializer=lambda o: encoder.encode(o),
    json_deserializer=lambda s: json.loads(s, object_hook=from_dict),
)

metadata = MetaData(bind=mysql_engine)

task_execution: Table = Table(
    'task_executions',
    metadata,
    Column('id', INTEGER, primary_key=True),
    Column('system', VARCHAR(length=255), nullable=False, index=True),
    Column('level', INTEGER, nullable=False, default=1, index=True),
    Column('task', JSON(none_as_null=True), nullable=False),
    Column('error', JSON(none_as_null=True), nullable=True),
    Column('resolving_tasks', JSON(none_as_null=True), nullable=True),
)


# Whether or not tables have been created.
_tables_created = False


def _with_tables(func: Callable) -> Callable:
    """Lazily create tables with sqlalchemy.

    This context manager returns a wrapper that creates tables before
    delegating.
    """

    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        """Create tables and then delegate to the wrapped function."""
        try:
            if not _tables_created:
                metadata.create_all()
        except OperationalError:
            logger.exception('Error while creating database tables.')
            raise
        else:
            return func(*args, **kwargs)

    return wrapper


def _cast_json(v: Any) -> Union[Cast, None]:
    """Cast a value to a JSON column for comparison.

    Parameters
    ----------
    v : Any
        Any value.

    Returns
    -------
        None if v is None, else a sqlalchemy cast to JSON.
    """
    if v is None:
        return None
    return cast(v, JSON)


@_with_tables
def insert_task_executions(executions: Iterable[ResolvingTaskTuple]):
    """Insert task executions into the knowledge base.

    If an execution already exists, the existing definition will take
    precedence and the new definition will not be inserted again (no
    duplicates). Note that the backing schema does not enforce uniqueness
    due to limitations on the JSON column type, so we do not use transactions
    and race conditions are possible if multiple processes ar running.

    Parameters
    ----------
    executions : Iterable[ResolvingTaskTuple]
        Concrete task executions, along with any errors and resolving tasks.
    """
    connection: Connection
    with mysql_engine.connect() as connection:
        for level, task, error, resolving_tasks in executions:

            # Determine if the task execution already exists in the knowledge
            # base.
            where_conditions = [
                task_execution.c.task == _cast_json(task),
                task_execution.c.error == _cast_json(error),
                task_execution.c.resolving_tasks == _cast_json(
                    resolving_tasks,
                ),
            ]
            if level is not None:
                where_conditions.append(task_execution.c.level == level)

            exists_query = select(
                select([
                    task_execution.c.id,
                ])
                .where(*where_conditions)
                .exists()
            )
            exists = next(connection.execute(exists_query))[0]

            # If it does exist, do not add again.
            if exists:
                msg_parts = [
                    'Task execution already exists, skipping:',
                    f'    task={task}',
                ]

                if error:
                    msg_parts.append(f'    error={error}'.rstrip())

                if resolving_tasks:
                    resolving_tasks_str = ', '.join(map(str, resolving_tasks))
                    msg_parts.append(
                        f'    resolving_tasks={resolving_tasks_str}'
                    )

                logger.verbose('\n'.join(msg_parts))
                continue

            # Insert into the knowledge base.
            attributes = {
                'system': task.system,
                'task': task,
                'error': error,
                'resolving_tasks': resolving_tasks,
            }
            if level is not None:
                attributes['level'] = level

            insert_query = task_execution.insert(attributes)
            connection.execute(insert_query)


@_with_tables
def get_configuration_tasks(system: Optional[Union[ConfigurationSystem, str]],
                            level: Optional[int] = None,
                            ) -> set[ConfigurationTask]:
    """Get all available configuration tasks for a configuration system.

    Parameters
    ----------
    system : Optional[Union[ConfigurationSystem, str]
        The name of a supported configuration system, or the Synth enum value
        representing that configuration system. If not specified tasks for all
        systems will be returned.
    level : Optional[int]
        Task level to use in search. If None, then all levels will be used.

    Returns
    -------
    set[ConfigurationTask]
        All known configuration tasks. Will be filtered to those belonging to
        ``system`` if provided.

    Raises
    ------
    ValueError
        Raised if ``system`` is not a valid value for ``ConfigurationSystem``.
    """
    # Start a query for all distinct tasks known to the knowledge base.
    query = select(distinct(task_execution.c.task))

    # If system was specified, convert it to the enum to verify it's valid and
    # then add the constraint to the query where clause.
    if system is not None:
        system = ConfigurationSystem(system)
        query = query.where(task_execution.c.system == system.value)

    # If level was specified, add a constraint to the where clause.
    if level is not None:
        query = query.where(task_execution.c.level == level)

    # Return the set of all tasks.
    return {result.task for result in query.execute()}


@_with_tables
def _resolving_tasks(task_id: int) -> list[ConfigurationTask]:
    """Get all associated resolving tasks without changes.

    Parameters
    ----------
    task_id : int
        Task id.

    Returns
    -------
    list[ConfigurationTask]
        All resolving tasks associated with the task. The resolving tasks
        will have changes excluded.
    """
    logger.debug(f'Loading resolving tasks for task with id `{task_id}`.')
    return [
        task.no_changes()
        for task in (
            select([task_execution.c.resolving_tasks])
            .where(task_execution.c.id == task_id)
            .execute()
            .one()
            .resolving_tasks
        )
    ]


# TODO Cases like touch dir1/dir2/dir3/file where the resolving task is either
#      mkdir -p dir1/dir2/dir3 (or a sequence of mkdir) may still prove
#      challenging because the values don't match exactly. We could try falling
#      back to traditional sequence alignment algorithms, either to try and
#      extract matching portions between task arguments and the error to
#      generate error arguments up front, or between two errors to try and
#      infer a mapping between them and possible arguments.
@_with_tables
def get_resolving_tasks(task: ConfigurationTask,
                        error: ConfigurationTaskError,
                        ) -> list[tuple[ConfigurationTask,
                                        ConfigurationTaskArgumentMapping]]:
    """Get the configuration tasks for resolving an error.

    Lookup for tasks resolving errors is done by the doing the following:

    1. Load all tasks/errors for the configuration system from the errors
       database. These errors may be optionally filtered if there is a
       good/reasonable filtering criterion.
    2. Look for a matching task from the errors database.
       2.1. If an exact match is found, use the exact match. The returned
            mapping will be empty.
       2.2. If an exact match is not found, use a task that ``task`` can map
            to. Apply this new mapping to ``error``. The returned mapping will
            be the inverse of this new mapping.
    3. Find an exact matching error in the matching task's associated errors.
       Return the list of resolving tasks plus the mapping selected from
       the previous step.

    Parameters
    ----------
    task : ConfigurationTask
        The configuration task that failed.
    error : ConfigurationTaskError
        An error caused by running the configuration task.

    Returns
    -------
    list[tuple[ConfigurationTask, ConfigurationTaskArgumentMapping]]
        A list of configuration tasks known to resolve the error.
    """
    # TODO Test without multiprocessing?
    # TODO Test with swap and swappiness=0?

    logger.verbose(f'Loading errors for task `{task}`.')
    # Construct the query for all task errors from the same system as the
    # input error.
    query = (
        select([
            task_execution.c.id,
            task_execution.c.task,
            task_execution.c.error,
        ])
        .distinct()
        .where(
            task_execution.c.system == task.system.value,
            task_execution.c.task['value']['executable'] == task.executable,
            task_execution.c.error.is_not(None)
        )
        .execution_options(stream_results=True, max_row_buffer=1)
    )

    # Create an empty mapping for reference.
    empty_mapping = ConfigurationTaskArgumentMapping()

    # Search for exact and mapped matches.
    exact_matches = []
    mapped_matches = []
    for result in query.execute().yield_per(1):

        # Unpack.
        other_id = result.id
        other_task = result.task
        other_error = result.error.from_arguments(
            result.task.configuration_task_arguments
        )

        logger.verbose(f'Comparing to task: `{other_task}`.')

        # If the other task is exactly equal to the mapped input task, then
        # preserve the mapped input error and use the empty mapping to for
        # resolving tasks. Append them and the other task info to the list of
        # exact matches.
        if task == other_task:
            logger.verbose(f'Exact task match found: `{other_task}`.')
            exact_matches.append((
                error,
                empty_mapping,
                other_error,
                _resolving_tasks(other_id),
            ))
            continue

        # If the other task is not exactly equal, attempt to generate a
        # secondary mapping from the mapped input task. If a secondary mapping
        # can be generated, apply it to the mapped error and use the inverse
        # secondary mapping for resolving tasks. Append them and the other
        # task info to the list of mapped matches.
        secondary_mapping = task.map_to_task(other_task)
        if secondary_mapping:
            logger.verbose(f'Mapped task match found: `{other_task}`.')
            mapped_matches.append((
                error.from_mapping(secondary_mapping),
                secondary_mapping.invert(),
                other_error,
                _resolving_tasks(other_id),
            ))
            continue

        logger.verbose(f'No task match found: `{other_task}`.')

    # Look through all matches for cases where the mapped error (which may have
    # a secondary mapping applied) is exactly equal to the error for the other
    # task. If found, return all resolving tasks paired with the resolving
    # mapping.
    if exact_matches or mapped_matches:
        logger.verbose('Searching for a matching error.')

    matches = chain(exact_matches, mapped_matches)
    for final_error, final_mapping, other_error, resolving_tasks in matches:
        if final_error == other_error:
            logger.verbose(f'Exact match for `{other_error}`.')
            return list(zip_longest(
                resolving_tasks,
                [],
                fillvalue=final_mapping,
            ))
        else:
            logger.verbose(f'No error match for `{other_error}`.')
    return []
