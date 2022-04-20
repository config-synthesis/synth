"""Synth test configuration."""

# Imports.
import logging
from contextlib import ExitStack
from typing import Generator
from unittest.mock import Mock, patch

import pytest
from docker import client
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.sql import Executable

from synth.synthesis import knowledge_base as kb


@pytest.fixture(autouse=True)
def docker_client() -> Generator[Mock, None, None]:
    """Mock ``client.from_env``.

    This fixture prevents tests from connecting to a live Docker engine.
    """
    with patch.object(client, 'from_env') as mock:
        docker_client = mock()
        container = docker_client.containers.run.return_value
        container.exec_run.return_value = (0, ('', ''))

        yield docker_client


@pytest.fixture(autouse=True)
def tables_created() -> Generator[None, None, None]:
    """Set tables created metadata.

    This fixture prevents the knowledge base from trying to create tables.
    """
    tables_created = kb._tables_created
    kb._tables_created = True
    try:
        yield
    finally:
        kb._tables_created = tables_created


@pytest.fixture(autouse=True)
def sqlalchemy_execute() -> Generator[Mock, None, None]:
    """Mock sqlalchemy execute methods.

    This prevents database calls from being made.
    """
    with ExitStack() as stack:
        mock = Mock()
        stack.enter_context(patch.object(Engine, 'execute', new=mock))
        stack.enter_context(patch.object(Connection, 'execute', new=mock))
        stack.enter_context(patch.object(Executable, 'execute', new=mock))

        mock.return_value.__iter__ = Mock(side_effect=lambda: iter(()))
        mock.return_value.yield_per.return_value = mock.return_value

        yield mock


@pytest.fixture(autouse=True)
def sqlalchemy_connect(sqlalchemy_execute: Mock
                       ) -> Generator[Mock, None, None]:
    """Mock sqlalchemy connections.

    Parameters
    ----------
    sqlalchemy_execute : Mock
        The mock sqlalchemy execute method.
    """
    with patch.object(Engine, 'connect') as mock:
        enter_mock = mock().__enter__()
        enter_mock.execute = sqlalchemy_execute
        yield enter_mock


@pytest.fixture(autouse=True)
def disable_logging() -> Generator[None, None, None]:
    """Disable all logging utilities during testing."""
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(logging.NOTSET)
