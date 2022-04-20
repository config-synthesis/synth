"""Synth Ansible playbook parsing."""


# Imports.
from collections.abc import Generator, Mapping, Sequence, Set
from contextlib import contextmanager, ExitStack, nullcontext
from itertools import chain
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from textwrap import indent
from typing import Any, Optional, Union


import yaml
from ansible import constants as ansible_constants
from ansible.errors import AnsibleUndefinedVariable
from ansible.executor.play_iterator import PlayIterator
from ansible.inventory.manager import InventoryManager
from ansible.parsing.dataloader import DataLoader
from ansible.playbook import Playbook
from ansible.playbook.play_context import PlayContext
from ansible.playbook.task import Task
from ansible.template import Templar
from ansible.vars.manager import VariableManager
from docker.types import Mount

from synth.logging import logger
from synth.synthesis.classes import (
    ConfigurationSystem,
    ConfigurationTask,
    frozendict,
)
from synth.synthesis.configuration_scripts.classes import ParseResult


# Constants
IGNORED_TASKS = {
    'gather_facts',
    'meta',
}


@contextmanager
def _playbook_file(playbook: str) -> Generator[Path, None, None]:
    """Create a temporary playbook file for playbook contents.

    Parameters
    ----------
    playbook : str
        Contents of an Ansible playbook file.

    Yields
    ------
    Path
        Temporary playbook file path.
    """
    with TemporaryDirectory() as path:
        file_path = Path(path) / 'playbook.yml'
        file_path.write_text(playbook)
        yield file_path


def _stringify(v: Any) -> Any:
    """Convert strings in Ansible values to the ``str`` class.

    This is necessary because Ansible has its own unicode class that subclasses
    str.

    Parameters
    ----------
    v : Any
        Any object returned by the Ansible parser.

    Returns
    -------
    Any
        That value with all internal unicode objects as str.
    """
    if type(v) == str:
        return v
    elif issubclass(type(v), str):
        return str(v)
    elif isinstance(v, Mapping):
        return {_stringify(key): _stringify(value) for key, value in v.items()}
    elif isinstance(v, Sequence):
        return [_stringify(value) for value in v]
    else:
        return v


@contextmanager
def _config(overrides: Mapping[str, Any]) -> Generator[None, None, None]:
    """Override Ansible config constants.

    The previous configuration will be restored when exiting the context.

    Parameters
    ----------
    overrides : Mapping[str, Any]
        Config constants to override.
    """
    old_config = {}

    for key, value in overrides.items():
        old_config[key] = getattr(ansible_constants, key, None)
        ansible_constants.set_constant(key, value)

    try:
        yield
    finally:
        for key, value in old_config.items():
            ansible_constants.set_constant(key, value)


def parse_host_patterns(playbook: Path) -> Set[str]:
    """Parse playbook all host patterns from a playbook.

    This includes any additional playbooks that are included via the
    ``import_playbook`` task.

    Parameters
    ----------
    playbook : Path
        Path to the playbook file.

    Returns
    -------
    Set[str]
        Set of all host patterns from all plays.
    """
    # Read playbook
    with open(playbook, 'r') as fd:
        playbook_data = yaml.full_load(fd)

    # Start set of all parsed host patterns
    host_patterns = set()

    # Get host patterns from each play
    for play in playbook_data:

        # Union any hosts
        if 'hosts' in play:
            host_patterns |= {
                host for host in play['hosts'] if not host.startswith('!')
            }

        # Get any additional imported playbooks
        if 'import_playbook' in play:
            new_playbook = playbook.parent / play['import_playbook']
            host_patterns |= parse_host_patterns(new_playbook)

    # Return
    return host_patterns


def parse_ansible_playbook(playbook: Union[Path, str],
                           context: Optional[Path] = None,
                           roles_paths:
                           Optional[Sequence[Union[Path, str]]] = None,
                           roles_paths_behavior: str = 'override',
                           ) -> ParseResult:
    """Parse an Ansible playbook for Ansible configuration tasks.

    Parameters
    ----------
    playbook : Union[Path, str]
        The playbook to parse. This should either be the playbook text or a
        path to a readable file.
    context : Optional[Path]
        Path to an optional context directory. If specified, it will be used
        for additional metadata in the parse result.
    roles_paths : Optional[Sequence[Union[Path, str]]]
        One or more paths where Ansible should look for roles. If not provided
        the default behavior is to look for a roles directory in every ancestor
        directory of the playbook. To disable this, use ``roles_paths = []``
        with ``roles_paths_behavior = 'override'``.
    roles_paths_behavior : str
        How roles paths should be looked up. Only applicable if ``roles_paths``
        is not None. The default of ``override`` will disable the automatic
        roles path generation. ``append`` will enable it and append the
        discovered roles paths to the initially provided list.

    Raises
    ------
    ValueError
        Raised if the playbook is a path and does not exist.

    Returns
    -------
    ParseResult
        Configuration tasks and metadata parsed from the playbook.
    """
    if isinstance(playbook, Path):
        if not playbook.exists():
            raise ValueError('Playbook does not exist.')

        playbook_ctx = nullcontext(playbook)
    else:
        playbook_ctx = _playbook_file(playbook)

    with ExitStack() as stack:

        # Enter playbook context.
        playbook_path = stack.enter_context(playbook_ctx)
        logger.info(f'Parsing Ansible Playbook: {playbook_path}')

        if context is None:
            context = playbook_path.parent

        # Write a temporary inventory file so that all plays apply.
        inventory_lines = [
            '[local]',
            'localhost ansible_connection=local ansible_host=localhost',
            *chain.from_iterable(
                (f'[{host}]', f'[{host}:children]\nlocal')
                for host in parse_host_patterns(playbook_path)
            ),
        ]
        inventory_file = stack.enter_context(NamedTemporaryFile())
        inventory_file.write(
            bytes('\n'.join(inventory_lines), encoding='utf-8'),
        )
        inventory_file.flush()  # Guarantees other processes see writes

        # Get roles paths and enter the Ansible config context.
        parent_path = playbook_path
        if roles_paths is None or roles_paths_behavior == 'append':
            roles_paths = roles_paths or []

            while parent_path.parent != parent_path:
                parent_path = parent_path.parent
                roles_path = parent_path / 'roles'

                if roles_path.is_dir():
                    roles_paths.append(str(roles_path))
        stack.enter_context(_config({'DEFAULT_ROLES_PATH': roles_paths}))

        # Construct Ansible data loader and managers
        loader = DataLoader()
        inventory = InventoryManager(
            loader=loader,
            sources=inventory_file.name,
        )
        variable_manager = VariableManager(loader=loader)

        # Load playbook
        playbook = Playbook.load(
            playbook_path,
            loader=loader,
            variable_manager=variable_manager
        )

        # Process all plays
        tasks = []
        for play in playbook.get_plays():

            logger.info(f'Processing play: {play}')

            # Create a new play iterator to get tasks
            iterator = PlayIterator(
                play=play,
                play_context=PlayContext(play=play),
                variable_manager=variable_manager,
                all_vars=variable_manager.get_vars(play=play),
                inventory=inventory
            )

            # Get initial state and task for the playbook. Use host=localhost
            # as a dummy host for iteration.
            task: Task
            state, task = iterator.get_next_task_for_host(
                host=inventory.localhost,
            )

            # Process each playbook task
            while task:

                if task.action not in IGNORED_TASKS:
                    templar = Templar(
                        loader=loader,
                        variables=variable_manager.get_vars(
                            play=play,
                            task=task,
                        ),
                    )

                    # Render the variables in the task.
                    try:
                        task.name = templar.template(task.name)
                        task.args = templar.template(task.args)
                    except AnsibleUndefinedVariable as e:
                        # Use Jinja to programmatically access top-level
                        # variables. If they exist in the Templar environment
                        # you will need to recursively search through their
                        # values as well, since variables can reference other
                        # variables.
                        #
                        # from jinja2.meta import find_undeclared_variables
                        # jinja_ast = templar.environment.parse(task.args)
                        # variables = find_undeclared_variables(jinja_ast)

                        err_lines = str(e).splitlines()[:-1]
                        err_msg = indent('\n'.join(err_lines), '    ')

                        logger.info(
                            f'Encountered an undefined variable while parsing '
                            f'{task}.\n    {err_msg}'
                        )
                        break

                    logger.info(f'Parsed Task: {task}')
                    tasks.append(ConfigurationTask(
                        system=ConfigurationSystem.ANSIBLE,
                        executable=_stringify(task.action),
                        arguments=frozendict(_stringify(task.args)),
                        changes=frozenset(),
                    ))

                # Get next playbook task.
                state, task = iterator.get_next_task_for_host(
                    host=inventory.localhost,
                )

        mounts = [
            Mount(
                source=child.name,
                target=f'/root/runner/{child.name}',
                type='bind',
                read_only=True,
            )
            for child in context.glob('*')
            if (child.name != playbook_path.name
                and child.name != 'playbook.yml')
        ]

        return ParseResult(tasks=tasks, mounts=mounts)


def write_playbook(tasks: Sequence[ConfigurationTask],
                   hosts: str = 'localhost',
                   become: bool = True) -> str:
    """Write an Ansible playbook from a sequence of configuration tasks.

    Parameters
    ----------
    tasks : Sequence[ConfigurationTask]
        Configuration tasks for the Ansible playbook.
    hosts : str
        Playbook hosts.
    become : bool
        Ansible ``become`` value.

    Returns
    -------
    str
        Ansible playbook contents.
    """
    if any(task.system != ConfigurationSystem.ANSIBLE for task in tasks):
        raise ValueError(
            'Cannot write Ansible playbook. All tasks must be Ansible tasks',
        )

    return yaml.safe_dump([{
        'hosts': hosts,
        'become': become,
        'tasks': [
            {
                'name': f'Run {task.executable}',
                task.executable: dict(task.arguments),
            }
            for task in tasks
        ],
    }])
