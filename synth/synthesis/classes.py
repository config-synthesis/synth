"""Data classes in support of configuration synthesis."""


# Imports
from __future__ import annotations

import json
import re
from abc import ABC
from collections import Counter, defaultdict
from collections.abc import (
    Callable, Iterable, Iterator, Mapping, Sequence, Set
)
from dataclasses import dataclass, field
from enum import Enum, unique
from functools import cached_property, total_ordering
from itertools import chain, combinations, product
from multiprocessing import cpu_count, Pool
from typing import Any, Optional, Type, TypeVar, Union

import networkx as nx

from synth.logging import logger
from synth.synthesis.exceptions import MatchingException
from synth.util import shell
from synth.util.timeout import Timeout


# Types
T = TypeVar('T')
ArgumentPair = tuple['ConfigurationTaskArgument', 'ConfigurationTaskArgument']


# Constants.
LOG_INTERVAL = 100000
VERSION_REGEX = re.compile(
    r'(?:0|[1-9]\d*)'
    r'\.(?:0|[1-9]\d*)'
    r'(?:\.(?:0|[1-9]\d*))?'
    r'(?:-(?:'
    r'(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)'
    r'(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))'
    r'*))?'
    r'(?:\+(?:[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?'
)


@unique
class ConfigurationSystem(str, Enum):
    """A configuration system."""

    SHELL = 'shell'
    DOCKER = 'docker'
    ANSIBLE = 'ansible'


def _identity(value: str) -> str:
    """Return the original value.

    Parameters
    ----------
    value : str
        Input value.

    Returns
    -------
    str
        The input value.
    """
    return value


def _undo_path_replacement(value: str) -> str:
    """Undo a path replacement in a configuration task argument.

    Parameters
    ----------
    value : str
        The transformed value.

    Returns
    -------
    str
        A string value where `/` are replaced with `.`.
    """
    return value.replace('/', '.')


@dataclass(frozen=True, order=True)
class ConfigurationTaskArgument:
    """A configuration task arguments.

    Arguments represent values supplied to configuration tasks on invocation.
    """

    value: str
    original_type: type
    original_value: Any
    transformer: Callable[[str], str] = field(compare=False)
    pre_transform_value: str = field(compare=False)

    def __init__(self,
                 original_value: Any,
                 transformer: Callable[[str], str] = _identity,
                 pre_transform_value: str = None):
        """Initialize a new configuration task argument.

        Parameters
        ----------
        original_value : Any
            The original argument value.
        transformer : Callable[[str], str]
            A callable that accepts a value string and returns a transformed
            value string. The transformer is meant for use when an argument
            must go through a reverse transformation after being mapped.
            This callable must be a valid value for pickling, which usually
            means no lambdas.
        pre_transform_value : str
            The argument value pre-transform, if it was transformed. Must be
            defined if a transformer other than the identity function is
            provided.
        """
        object.__setattr__(self, 'original_value', original_value)
        object.__setattr__(self, 'original_type', type(original_value))
        object.__setattr__(self, 'value', str(original_value))
        object.__setattr__(self, 'transformer', transformer)
        object.__setattr__(self, 'pre_transform_value', pre_transform_value)

    def __str__(self) -> str:
        """Return a human-readable string representation for self."""
        return self.value


class ConfigurationTaskArgumentMapping:
    """A mapping of source to target configuration task arguments.

    Each mapping must represent a valid matching (each source only has one
    target and vice versa).
    """

    @classmethod
    def all_combinations(cls,
                         mappings:
                         Iterable[Iterable[ConfigurationTaskArgumentMapping]]
                         ) -> set[ConfigurationTaskArgumentMapping]:
        """Generate all valid mappings from the cartesian product.

        This method takes the cartesian product of input mapping iterables and
        returns all distinct and valid merged mappings for every set of
        elements in the product.

        Parameters
        ----------
        mappings : Iterable[Iterable[ConfigurationTaskArgumentMapping]]
            Input mapping iterables.

        Returns
        -------
        set[ConfigurationTaskArgumentMapping]
            All valid merged mappings from the product of inputs.
        """
        mappings = list(mappings)
        if not mappings:
            return set()

        merged_mappings = set()
        for elements in product(*mappings):
            try:
                mapping = cls.merge_all(elements)
            except MatchingException:
                pass
            else:
                merged_mappings.add(mapping)

        return merged_mappings

    @classmethod
    def merge_all(cls,
                  mappings: Iterable[ConfigurationTaskArgumentMapping]
                  ) -> ConfigurationTaskArgumentMapping:
        """Create a new mapping from multiple source mappings.

        Parameters
        ----------
        mappings : Iterable[ConfigurationTaskArgumentMapping]
            All source mappings.

        Returns
        -------
        ConfigurationTaskArgumentMapping
            A mapping generated by merging all mappings from ``mappings``.

        Raises
        ------
        MatchingException
            Raised if the merged mapping would
        """
        return ConfigurationTaskArgumentMapping(chain.from_iterable(
            mapping.source_arguments.items()
            for mapping in mappings
        ))

    def __init__(self,
                 mapping: Iterable[ArgumentPair] = ()):
        """Create a new mapping.

        Parameters
        ----------
        mapping : Iterable[ArgumentPair]
            An initial mapping of source to target arguments.

        Raises
        ------
        MatchingException
            Raised if the initial mapping is not a valid matching.
        """
        self.source_arguments: dict[ConfigurationTaskArgument,
                                    ConfigurationTaskArgument] = {}
        self.target_arguments: dict[ConfigurationTaskArgument,
                                    ConfigurationTaskArgument] = {}

        for pair in mapping:
            self.add_pair(pair)

    def __hash__(self) -> int:
        """Hash self.

        Returns
        -------
        int
            A hash generated from all (source, target) pairs.
        """
        return hash(frozenset(self.source_arguments.items()))

    def __eq__(self, other: Any) -> bool:
        """Determine if one mapping is equal to another.

        Parameters
        ----------
        other : Any
            True iff ``other`` is a ``ConfigurationTaskArgumentMapping`` with
            the same source and target argument mappings.
        """
        if not isinstance(other, ConfigurationTaskArgumentMapping):
            return NotImplemented

        return (
            self.source_arguments == other.source_arguments
            and self.target_arguments == other.target_arguments
        )

    def __repr__(self) -> str:
        """Return a representation for self."""
        return repr(self.source_arguments)

    def __str__(self) -> str:
        """Return a human-readable string representation for self."""
        return str({
            str(k): str(v)
            for k, v in self.source_arguments.items()
        })

    def add_pair(self,
                 pair: tuple[ConfigurationTaskArgument,
                             ConfigurationTaskArgument]):
        """Add a new pair to the mapping.

        Parameters
        ----------
        pair : tuple[ConfigurationTaskArgument, ConfigurationTaskArgument]
            The pair or (source, target) arguments to add.

        Raises
        ------
        MatchingException
            Raised if the addition of the pair would create an invalid
            matching.
        """
        a, b = pair
        a_map = self.source_arguments.get(a, None)
        b_map = self.target_arguments.get(b, None)
        if (a_map is not None and a_map != b
                or b_map is not None and b_map != a):
            raise MatchingException(
                'Added pairs would create an invalid matching.'
            )
        self.source_arguments[a] = b
        self.target_arguments[b] = a

    def invert(self) -> ConfigurationTaskArgumentMapping:
        """Invert the argument mapping.

        The resulting mapping will be ``target`` => ``source`` instead of
        ``source`` => ``target``.

        Returns
        -------
        ConfigurationTaskArgumentMapping
            Inverted argument mapping.
        """
        return ConfigurationTaskArgumentMapping(
            (b, a)
            for a, b in self.source_arguments.items()
        )

    def merge(self, other: ConfigurationTaskArgumentMapping
              ) -> ConfigurationTaskArgumentMapping:
        """Create a new mapping by merging `self` with `other`.

        Parameters
        ----------
        other : ConfigurationTaskArgumentMapping
            The mapping to merge with `self`.

        Returns
        -------
        ConfigurationTaskArgumentMapping
            A mapping generated by merging `self` and `other`.

        Raises
        ------
        MatchingException
            Raised if the merged mapping would
        """
        return ConfigurationTaskArgumentMapping(chain(
            self.source_arguments.items(),
            other.source_arguments.items(),
        ))


@dataclass(frozen=True, order=True)
class SyntheticValue:
    """A synthetic value.

    Synthetic values are values that may contain one or more dynamic segments.
    Segments are specified as one or more configuration task arguments that
    would be supplied when a configuration task is invoked.
    """

    original_value: Any
    original_type: type
    arguments: frozenset[ConfigurationTaskArgument]
    parts: tuple[Union[str, ConfigurationTaskArgument], ...]

    def __init__(self,
                 original_value: Any,
                 arguments: frozenset[ConfigurationTaskArgument]):
        """Initialize a synthetic value.

        Parameters
        ----------
        original_value : Any
            The original value being wrapped by this synthetic value.
        arguments : frozenset[ConfigurationTaskArgument]
            One or more arguments that may appear in ``original_value``.
        """
        object.__setattr__(self, 'original_value', original_value)
        object.__setattr__(self, 'original_type', type(original_value))

        # Convert the original value to a string.
        ov_str = str(original_value)

        # Create a mutable set for arguments.
        arguments = set(arguments)

        # Create arguments for version numbers, even if they aren't part of the
        # provided arguments.
        versions = VERSION_REGEX.findall(ov_str)
        for version in versions:
            arguments.add(ConfigurationTaskArgument(original_value=version))

        # Create arguments for common substitutions.
        new_args = set()
        for arg in arguments:
            if (isinstance(arg.original_value, str)
                    and '.' in arg.original_value):
                replacement = arg.original_value.replace('.', '/')
                if replacement in ov_str:
                    new_args.add(ConfigurationTaskArgument(
                        original_value=replacement,
                        transformer=_undo_path_replacement,
                        pre_transform_value=arg.original_value,
                    ))
        arguments.update(new_args)

        # Sort all arguments by length in descending order.
        arguments = sorted(
            arguments,
            key=lambda a: len(a.value),
            reverse=True,
        )

        if ov_str:
            parts = [ov_str]
        else:
            parts = []
        for argument in arguments:
            if not argument.value:
                continue
            new_parts = []
            for part in parts:
                if isinstance(part, ConfigurationTaskArgument):
                    new_parts.append(part)
                    continue
                before, sep, after = part.partition(argument.value)
                new_parts.append(before)
                while sep:
                    new_parts.append(argument)
                    before, sep, after = after.partition(argument.value)
                    new_parts.append(before)
            parts = list(filter(lambda p: p, new_parts))

        used_arguments = {
            a for a in parts if isinstance(a, ConfigurationTaskArgument)
        }
        object.__setattr__(self, 'parts', tuple(parts))
        object.__setattr__(self, 'arguments', frozenset(used_arguments))

    def __str__(self) -> str:
        """Represent self as a string.

        Returns
        -------
        str
            ``self.original_value``.
        """
        return str(self.original_value)

    def from_mapping(self,
                     mapping: ConfigurationTaskArgumentMapping
                     ) -> SyntheticValue:
        """Create a new synthetic value from ``self`` and a mapping.

        Parameters
        ----------
        mapping : ConfigurationTaskArgumentMapping
            A mapping of arguments in ``self`` to arguments in the new
            synthetic value. This mapping must include all arguments in
            ``self``.

        Returns
        -------
        SyntheticValue
            A new synthetic value with mapped arguments replaced.

        Raises
        ------
        KeyError
            Raised if ``mapping`` does not contain an argument from ``self``.
        """
        sv = object.__new__(SyntheticValue)

        parts = tuple(
            mapping.source_arguments.get(part, part)
            if isinstance(part, ConfigurationTaskArgument)
            else part
            for part in self.parts
        )
        arguments = frozenset(
            part
            for part in parts if isinstance(part, ConfigurationTaskArgument)
        )
        original_value = self.original_type(''.join(
            part.original_value
            if isinstance(part, ConfigurationTaskArgument)
            else part
            for part in parts
        ))

        sv.__dict__.update({
            'parts': parts,
            'arguments': arguments,
            'original_type': self.original_type,
            'original_value': original_value,
        })
        return sv

    def map_to_primitive(self,
                         other: Any
                         ) -> set[ConfigurationTaskArgumentMapping]:
        """Match self to a primitive value.

        Parameters
        ----------
        other : Any
            The other primitive to match self to.

        Returns
        -------
        set[ConfigurationTaskArgumentMapping]
            All valid mapping of configuration task arguments from ``self``
            into ``other``. This will be the empty set if there are no
            mappings (an invalid alignment). If there is a valid alignment but
            no arguments, this will be the set containing the empty mapping.

        Raises
        ------
        ValueError
            Raised if the type of ``other`` is not compatible with
            ``self.original_type``.
        """
        if not issubclass(type(other), self.original_type):
            raise ValueError('Primitive must match the original type.')

        # If other exactly matches `self.original_value`, return a mapping to
        # self.
        if self.original_value == other:
            return {
                ConfigurationTaskArgumentMapping(
                    (a, a) for a in self.arguments
                ),
            }

        # If the values do not match and there are no arguments, there cannot
        # be any valid mappings.
        if not self.arguments:
            return set()

        # Get sequences for checking alignment.
        self_sequence = list(chain.from_iterable(
            part if not isinstance(part, ConfigurationTaskArgument) else [part]
            for part in self.parts
        ))
        self_len = len(self_sequence)
        other_sequence = str(other)
        other_len = len(other_sequence)

        # Create the set of all mappings found, set the start state, and then
        # begin searching for valid alignments.
        mappings = set()
        states = [(0, 0, ConfigurationTaskArgumentMapping())]
        while states:
            self_idx, other_idx, mapping = states.pop()

            # Advance until the sequences are not aligned (potentially by
            # hitting the end of one of them before the other) or both
            # sequences have been consumed.
            while (self_idx < self_len
                   and other_idx < other_len
                   and self_sequence[self_idx] == other_sequence[other_idx]):
                self_idx += 1
                other_idx += 1

            # If both sequences have been consumed, then this must be a valid
            # alignment, save the mapping.
            if self_idx == self_len and other_idx == other_len:
                mappings.add(mapping)
                continue

            # If we hit the end of one sequence (but not both), this is an
            # invalid alignment.
            if self_idx == self_len or other_idx == other_len:
                continue

            # Get the current value (potentially an argument).
            arg = self_sequence[self_idx]

            # If the sequences do not align on a non-argument value, this is
            # an invalid alignment.
            if not isinstance(arg, ConfigurationTaskArgument):
                continue

            # If the argument has already been mapped, verify that the mapped
            # value can be immediately consumed from other. If it can, push a
            # state, otherwise this is an invalid alignment.
            if arg in mapping.source_arguments:
                mapped = mapping.source_arguments[arg]
                other_value = other[other_idx:other_idx + len(mapped.value)]
                if mapped.value == other_value:
                    states.append((
                        self_idx + 1,
                        other_idx + len(mapped.value),
                        mapping
                    ))
                continue

            # Find all the possible starting indices for the next search state.
            # 1. If the argument is at the end of the synthetic value, the only
            #    possible mapping is to the rest of other. The next state falls
            #    off the end of the sequence.
            # 2. If the next value is also an argument, then the boundary
            #    between them could be at any place up until the last time the
            #    next non-argument value aligns with `other`.
            # 3. If the next value is not an argument, then it could be aligned
            #    with any subsequent occurrence in `other`.
            if self_idx + 1 == self_len:
                indices = [other_len]
            elif isinstance(self_sequence[self_idx + 1],
                            ConfigurationTaskArgument):
                end = other_len + 1

                next_idx = self_idx + 1
                while (next_idx < self_len
                       and isinstance(self_sequence[next_idx],
                                      ConfigurationTaskArgument)):
                    next_idx += 1

                if next_idx < self_len:
                    try:
                        end = 1 + other_sequence.rindex(
                            self_sequence[next_idx],
                            other_idx,
                        )
                    except ValueError:
                        # If there is no match for the next value in `other`,
                        # then this cannot be a valid alignment.
                        continue

                indices = list(range(other_idx, end))
            else:
                indices = [
                    k for k in range(other_idx, other_len)
                    if other_sequence[k] == self_sequence[self_idx + 1]
                ]

            # Append a new search state for every possible starting index.
            for idx in indices:
                try:
                    merged = mapping.merge(ConfigurationTaskArgumentMapping([
                        (arg, ConfigurationTaskArgument(
                            original_value=other_sequence[other_idx:idx]))
                    ]))
                    states.append((self_idx + 1, idx, merged))
                except MatchingException:
                    pass

        return mappings


@dataclass(frozen=True, order=True)
class ConfigurationTask:
    """A configuration task.

    Configuration tasks represent some generic action which results in a
    configuration change. Tasks belong to some configuration system, have an
    executable and arguments, and contain a set of changes.
    """

    system: ConfigurationSystem
    executable: str
    arguments: Union[tuple[str, ...], frozendict]
    changes: frozenset[ConfigurationChange]

    def __init__(self,
                 system: ConfigurationSystem,
                 executable: str,
                 arguments: Union[tuple[str, ...], frozendict],
                 changes: frozenset[ConfigurationChange]):
        """Perform post-init setup."""
        object.__setattr__(self, 'system', system)
        object.__setattr__(self, 'executable', executable)
        object.__setattr__(self, 'arguments', arguments)

        # Parse all task arguments.
        if isinstance(self.arguments, Sequence):
            arguments = {
                ConfigurationTaskArgument(original_value=argument)
                for argument in self.arguments
            }
        elif isinstance(self.arguments, Mapping):
            arguments = set()
            nodes = list(self.arguments.values())
            while nodes:
                node = nodes.pop()
                if isinstance(node, Sequence) and not isinstance(node, str):
                    nodes += node
                elif isinstance(node, Mapping):
                    nodes += node.values()
                else:
                    arguments.add(ConfigurationTaskArgument(
                        original_value=node,
                    ))
        else:
            raise ValueError('Unsupported arguments type.')

        # Save all configuration task arguments.
        configuration_task_arguments = frozenset(arguments)
        object.__setattr__(
            self,
            'configuration_task_arguments',
            configuration_task_arguments,
        )

        # Convert changes based on arguments.
        changes = frozenset({
            change.from_arguments(configuration_task_arguments)
            for change in changes
        })
        object.__setattr__(self, 'changes', changes)

    def __str__(self) -> str:
        """Represent self as a string.

        Returns
        -------
        str
            A formatted version of the configuration system name, executable,
            arguments, and configuration changes.
        """
        arguments = self.arguments
        if isinstance(self.arguments, tuple):
            arguments = shell.join(self.arguments)

        return f'{self.system.value}: {self.executable} {arguments}'

    def no_changes(self) -> ConfigurationTask:
        """Copy the configuration task without changes.

        Returns
        -------
        ConfigurationTask
            A new configuration task that is the same as the original excluding
            changes.
        """
        return ConfigurationTask(
            system=self.system,
            executable=self.executable,
            arguments=self.arguments,
            changes=frozenset(),
        )

    def from_mapping(self,
                     mapping: ConfigurationTaskArgumentMapping
                     ) -> ConfigurationTask:
        """Create a new configuration task from a mapping.

        Parameters
        ----------
        mapping : ConfigurationTaskArgumentMapping
            A mapping of arguments in ``self`` to arguments in the new task.
            This mapping must include all arguments in ``self``.

        Returns
        -------
        ConfigurationTask
            A new configuration task with arguments from the mapping.
        """
        configuration_task_arguments = set()
        if isinstance(self.arguments, Sequence):
            arguments = []
            for value in self.arguments:
                arg = ConfigurationTaskArgument(original_value=value)
                if arg in mapping.source_arguments:
                    mapped_arg = mapping.source_arguments[arg]
                    source_arg = mapping.target_arguments[mapped_arg]
                    arguments.append(source_arg.transformer(mapped_arg.value))
                    configuration_task_arguments.add(mapped_arg)
                else:
                    for source_arg in mapping.source_arguments:
                        if source_arg.pre_transform_value == value:
                            mapped_arg = mapping.source_arguments[source_arg]
                            arguments.append(source_arg.transformer(
                                mapped_arg.value,
                            ))
                            configuration_task_arguments.add(mapped_arg)
                            break
                    else:
                        arguments.append(value)
                        configuration_task_arguments.add(arg)
            arguments = tuple(arguments)
        elif isinstance(self.arguments, Mapping):
            arguments = dict(self.arguments)
            nodes = [(arguments, key) for key in arguments.keys()]
            while nodes:
                parent, key = nodes.pop()
                child = parent[key]

                if isinstance(child, Sequence) and not isinstance(child, str):
                    nodes += [(child, i) for i in range(len(child))]
                elif isinstance(child, Mapping):
                    nodes += [(child, key) for key in child.keys()]
                else:
                    arg = ConfigurationTaskArgument(original_value=child)
                    if arg in mapping.source_arguments:
                        mapped_arg = mapping.source_arguments[arg]
                        source_arg = mapping.target_arguments[mapped_arg]
                        parent[key] = source_arg.transformer(mapped_arg.value)
                        configuration_task_arguments.add(mapped_arg)
                    else:
                        for source_arg in mapping.source_arguments:
                            if source_arg.pre_transform_value == child:
                                mapped_arg = mapping.source_arguments[
                                    source_arg
                                ]
                                parent[key] = source_arg.transformer(
                                    mapped_arg.value,
                                )
                                configuration_task_arguments.add(mapped_arg)
                            break
                        else:
                            configuration_task_arguments.add(arg)
            arguments = frozendict(arguments)
        else:
            raise ValueError()

        changes = frozenset({
            change.from_mapping(mapping)
            for change in self.changes
        })

        task = object.__new__(ConfigurationTask)
        object.__setattr__(task, 'system', self.system)
        object.__setattr__(task, 'executable', self.executable)
        object.__setattr__(task, 'arguments', arguments)
        object.__setattr__(task, 'changes', changes)
        object.__setattr__(
            task,
            'configuration_task_arguments',
            frozenset(configuration_task_arguments),
        )
        return task

    def map_to_task(self,
                    other: ConfigurationTask
                    ) -> Optional[ConfigurationTaskArgumentMapping]:
        """Map this task to another task.

        Task mapping is performed on changes only. The mapping is the one for
        the largest intersection of their changes.

        Parameters
        ----------
        other : ConfigurationTask
            The other configuration task to map to.

        Returns
        -------
        ConfigurationTaskArgumentMapping
            Mapping of source task arguments to target task arguments.
        """
        intersection = ConfigurationChange.change_intersection(
            self.changes, other.changes
        )
        self_changes, other_changes, mapping = intersection
        return mapping


@dataclass(frozen=True)
@total_ordering
class DataclassWithSyntheticValues:
    """Common factory methods mixins for dataclasses with synthetic values.

    Classes using this mixin must be declared with @dataclass(frozen=True)
    and should define their attributes in the correct order for sorting,
    similar to how @dataclass(order=True) works. Unlike the default dataclass
    ordering, different subclasses can be compared with each other and will
    order by class name.

    In addition, a subclass' attributes can only be of the following types:

    1. A plain Python object.
    2. A SyntheticValue.
    3. An iterable containing DataclassWithSyntheticValue.

    The factory methods added by this mixin will take care of constructing new
    instances from primitives, constructing other instances with arguments,
    constructing other instances with mappings, and generating mappings.
    """

    @cached_property
    def _sort_tuple(self) -> tuple[Any]:
        """Get the dataclass' comparison fields as a tuple for sorting.

        This method is primarily intended for use with serializing sets into
        a deterministic order.

        Returns
        -------
        tuple[Any]
            A tuple of values for dataclass attributes marked for use in
            comparison. This tuple will be in definition order.
        """
        return tuple(
            getattr(self, field.name)
            for field in self.__dataclass_fields__.values()
            if field.compare
        )

    def __lt__(self, other: Any) -> bool:
        """Determine if ``self`` is less than another dataclass.

        Comparison is based on the class name plus the values of the dataclass
        attribute names and values.

        Parameters
        ----------
        other : Any
            The other value to compare to.

        Returns
        -------
        bool
            True if ``self`` is less than ``other``.
        """
        if not isinstance(other, DataclassWithSyntheticValues):
            return super().__lt__(other)

        if self.__class__.__name__ < other.__class__.__name__:
            return True
        elif self.__class__.__name__ > other.__class__.__name__:
            return False
        else:
            return self._sort_tuple < other._sort_tuple

    @classmethod
    def from_primitives(cls: Type[T],
                        *args,
                        arguments:
                        frozenset[ConfigurationTaskArgument] = frozenset(),
                        **kwargs) -> T:
        """Create a dataclass instance from primitives.

        Parameters
        ----------
        *args
            Primitive versions of all args accepted by ``__init__``.
        arguments : frozenset[ConfigurationTaskArgument]
            All arguments used to produce synthetic values.
        **kwargs
            Primitive versions of all kwargs accepted by ``__init__``.

        Returns
        -------
        T
            A dataclass instance with synthetic values based on the provided
            primitives and arguments.
        """
        # Convert all args into kwargs.
        kwargs.update({
            name: arg
            for name, arg in zip(cls.__dataclass_fields__, args)
        })

        # Create synthetic values or apply arguments.
        for name, arg in kwargs.items():
            if cls.__dataclass_fields__[name].type == SyntheticValue.__name__:
                kwargs[name] = SyntheticValue(
                    original_value=arg,
                    arguments=arguments,
                )
            elif isinstance(arg, Iterable) and not isinstance(arg, str):
                kwargs[name] = type(arg)(map(
                    lambda v: (
                        v.from_arguments(arguments)
                        if isinstance(v, DataclassWithSyntheticValues)
                        else v
                    ),
                    arg,
                ))

        # Return a new instance.
        return cls(**kwargs)

    def from_arguments(self: Type[T],
                       arguments: frozenset[ConfigurationTaskArgument]) -> T:
        """Create a dataclass instance from an instance and new arguments.

        Parameters
        ----------
        arguments : frozenset[ConfigurationTAskArguments]
            All arguments used to produce synthetic values.

        Returns
        -------
        T
            A new dataclass instance with synthetic values based on the
            original values in ``self`` plus the new arguments.
        """
        kwargs = {
            name: (
                arg.original_value
                if isinstance(arg, SyntheticValue)
                else arg
            )
            for name, arg in (
                (arg.name, getattr(self, arg.name))
                for arg in self.__dataclass_fields__.values()
            )
        }
        return self.from_primitives(arguments=arguments, **kwargs)

    def from_mapping(self: T, mapping: ConfigurationTaskArgumentMapping) -> T:
        """Create a dataclass instance from a mapping.

        Parameters
        ----------
        mapping : ConfigurationTaskArgumentMapping
            A mapping of arguments in ``self`` to arguments in the new
            instance.

        Returns
        -------
        T
            A dataclass instance with mapped arguments replaced.
        """
        # Get all dataclass arguments.
        args = (
            (arg.name, getattr(self, arg.name))
            for arg in self.__dataclass_fields__.values()
        )

        # Map all individual arguments.
        kwargs = {}
        for name, arg in args:
            if isinstance(arg, SyntheticValue):
                kwargs[name] = arg.from_mapping(mapping)
            elif isinstance(arg, Iterable) and not isinstance(arg, str):
                kwargs[name] = type(arg)(map(
                    lambda v: (
                        v.from_mapping(mapping)
                        if isinstance(v, DataclassWithSyntheticValues)
                        else v
                    ),
                    arg,
                ))
            else:
                kwargs[name] = arg

        # Return the new instance.
        return type(self)(**kwargs)

    def map_to_other(self: T,
                     other: T) -> set[ConfigurationTaskArgumentMapping]:
        """Map to another dataclass of the same type.

        Parameters
        ----------
        other : T
            The other dataclass to map to.

        Raises
        ------
        TypeError
            Raised if ``other`` is not of the same type as ``self``.

        Returns
        -------
        set[ConfigurationTaskArgumentMapping]
            All possible valid mappings.
        """
        if not isinstance(other, type(self)):
            raise TypeError(
                f'Other error must be of the same type ({type(self)}).'
            )

        # Bin dataclass args by type.
        primitive_args = []
        synthetic_args = []
        sequences = []
        sets = []
        for arg in self.__dataclass_fields__.values():
            arg_value = getattr(self, arg.name)
            other_value = getattr(other, arg.name)
            record = (arg_value, other_value)

            if isinstance(arg_value, SyntheticValue):
                synthetic_args.append(record)
            elif (isinstance(arg_value, Sequence)
                  and not isinstance(arg_value, str)):
                sequences.append(record)
            elif isinstance(arg_value, Set):
                sets.append(record)
            else:
                primitive_args.append(record)

        # If any of the primitive args differ, there can be no mapping.
        if any(self_arg != other_arg
               for self_arg, other_arg in primitive_args):
            return set()

        # If any of the dataclass containers differ in length,
        # there can be no mapping.
        if any(len(self_arg) != len(other_arg)
               for self_arg, other_arg in chain(sequences, sets)):
            return set()

        # Return all possible mappings from the synthetic values and the
        # dataclass containers.
        return ConfigurationTaskArgumentMapping.all_combinations(chain(
            (
                self_arg.map_to_primitive(other_arg.original_value)
                for self_arg, other_arg in synthetic_args
            ),
            (
                ConfigurationTaskArgumentMapping.all_combinations(
                    self_item.map_to_other(other_item)
                    for self_item, other_item in zip(self_arg, other_arg)
                )
                for self_arg, other_arg in sequences
            ),
            (
                ConfigurationTaskArgumentMapping.all_combinations(
                    mappings
                    for self_item, other_item in product(self_arg, other_arg)
                    if (mappings := self_item.map_to_other(other_item))
                )
                for self_arg, other_arg in sets
            )
        ))


class ConfigurationTaskError(ABC, DataclassWithSyntheticValues, Exception):
    """An error generated by running a configuration task."""

    system: ConfigurationSystem


@dataclass(frozen=True)
class AnsibleTaskError(ConfigurationTaskError):
    """An error generated by running an Ansible task."""

    changed: bool
    msg: SyntheticValue
    json_output: SyntheticValue
    system = ConfigurationSystem.ANSIBLE

    @classmethod
    def from_json(cls,
                  json_output: str,
                  arguments:
                  frozenset[ConfigurationTaskArgument] = frozenset(),
                  ) -> AnsibleTaskError:
        """Create an Ansible task error from primitives.

        Parameters
        ----------
        json_output : str
            Raw JSON output from running the task.
        arguments : frozenset[ConfigurationTaskArgument]
            Arguments used to convert *args and **kwargs to synthetic values.

        Returns
        -------
        AnsibleTaskError
            An Ansible task error with synthetic values based on the provided
            primitives and arguments.
        """
        parsed_output = frozendict(json.loads(json_output))
        return AnsibleTaskError.from_primitives(
            changed=parsed_output['changed'],
            msg=parsed_output['msg'],
            json_output=json_output,
            arguments=arguments,
        )

    def __str__(self) -> str:
        """Represent self as a string.

        Returns
        -------
        str
            The string value of ``self.msg``.
        """
        return str(self.msg)


@dataclass(frozen=True)
class ShellTaskError(ConfigurationTaskError):
    """An error generated by running a shell task."""

    exit_code: int
    stdout: SyntheticValue
    stderr: SyntheticValue
    system = ConfigurationSystem.SHELL

    def __str__(self) -> str:
        """Represent self as a string.

        Returns
        -------
        str
            The exit code and stderrr..
        """
        return f'{self.exit_code}: {self.stderr}'


def _map(pair: tuple[ConfigurationChange, ConfigurationChange],
         ) -> tuple[ConfigurationChange,
                    ConfigurationChange,
                    set[ConfigurationTaskArgumentMapping]]:
    """Map a pair of configuration changes.

    Parameters
    ----------
    pair : tuple[ConfigurationChange, ConfigurationChange]
        A (source, target) pair of changes to map.

    Returns
    -------
    ConfigurationChange
        The source change.
    ConfigurationChange
        The target change.
    set[ConfigurationTaskArgumentMapping]
        Valid mappings from the source to target change.
    """
    source_change, target_change = pair

    try:
        with Timeout(seconds=1):
            mappings = source_change.map_to_other(target_change)
    except (TypeError, TimeoutError):
        return source_change, target_change, set()
    else:
        return source_change, target_change, mappings


class ConfigurationChange(ABC, DataclassWithSyntheticValues):
    """A configuration change.

    Configuration changes represent some alteration of the computing
    environment. All configuration changes must have a type, and specific types
    of changes have different resulting effects.
    """

    @classmethod
    def _filter_change_set(cls,
                           changes: Set[ConfigurationChange],
                           ) -> set[ConfigurationChange]:
        """Create a filtered change set.

        This method is useful for discarding changes that are too big for
        computing a change intersection. Currently, only large FileChanges
        are removed.

        Parameters
        ----------
        changes : Set[ConfigurationChange]
            All changes.

        Returns
        -------
        set[ConfigurationChange]
            All changes, minus those that were discarded.
        """
        return {
            task
            for task in changes
            if (not isinstance(task, FileChange)
                or not any(len(change.content.original_value) > 500
                           for change in task.changes))
        }

    @classmethod
    def change_intersection(cls,
                            source: Set[ConfigurationChange],
                            target: Set[ConfigurationChange],
                            with_mapping: Optional[bool] = None,
                            ) -> tuple[set[ConfigurationChange],
                                       set[ConfigurationChange],
                                       ConfigurationTaskArgumentMapping]:
        """Compute the maximum intersection of configuration change sets.

        The intersection is computed by finding the maximum possible valid
        mapping from the source changes to the target changes.

        Parameters
        ----------
        source : Set[ConfigurationChange]
            Source configuration changes. These changes must have synthetic
            values generated from a configuration task's arguments.
        target : Set[ConfigurationChange]
            Target configuration changes. These changes must not have synthetic
            values.
        with_mapping : Optional[bool]
            Whether the change intersection should be computed by mapping
            changes to each other. If False, a standard set intersection will
            be taken. If True, the intersection from the maximum set of
            compatible mappings is returned. None (the default) acts like True
            unless the number of pairs in the change is

        Returns
        -------
        set[ConfigurationChange]
            All configuration changes from ``source`` that were selected as
            part of the maximum mapping.
        set[ConfigurationChange]
            All configuration changes from ``target`` that were selected as
            part of the maximum mapping.
        ConfigurationTaskArgumentMapping
            The mapping from source to target changes.
        """
        # Filter the source and target change sets.
        # source = cls._filter_change_set(source)
        # target = cls._filter_change_set(target)

        # Bin source and target changes by type.
        source_binned = defaultdict(set)
        for change in source:
            source_binned[type(change)].add(change)

        target_binned = defaultdict(set)
        for change in target:
            target_binned[type(change)].add(change)

        # Get all shared change types.
        shared_types = set(source_binned) & set(target_binned)

        # Compute the number of pairs that would be mapped if we computed the
        # intersection with mapping.
        num_pairs = sum(
            len(source_binned[change_type]) * len(target_binned[change_type])
            for change_type in shared_types
        )

        # If there are no pairs, return the empty intersection.
        if num_pairs == 0:
            return set(), set(), ConfigurationTaskArgumentMapping()

        # If using the exact intersection (no change mapping), compute it and
        # return immediately. Exact intersections use the empty mapping.
        if (with_mapping is False
                or (with_mapping is None and num_pairs > 25_000_000)):
            logger.debug('Using exact intersection.')
            intersection = source & target
            return (
                set(intersection),
                set(intersection),
                ConfigurationTaskArgumentMapping(),
            )

        # Compute the intersection using change mapping.
        logger.debug('Using intersection with mapping.')

        # Initialize storage variables.
        # 1. How often each mapping appears.
        # 2. The source changes associated with each mapping.
        # 3. The target changes associated with each mapping.
        counter = Counter()
        mapping_source_changes = defaultdict(set)
        mapping_target_changes = defaultdict(set)

        # For every (source, target) pair, check to see if the source change
        # can be mapped to the target change. If it can, then record the
        # mapping and its associated source and target changes.
        logger.debug('Finding mappings.')
        num_mappings_in_interval = 0
        pairs = chain.from_iterable(
            product(source_binned[change_type], target_binned[change_type])
            for change_type in shared_types
        )

        # If the number of pairs is high enough, run mapping through
        # multiprocessing. Otherwise, do it in-process.
        if num_pairs >= 500_000:
            # Determine the multiprocessing chunk size.
            chunksize = min(100_000, int(num_pairs / cpu_count()))

            with Pool(processes=cpu_count()) as pool:
                idx = 0
                for result in pool.imap_unordered(_map, pairs, chunksize):
                    source_change, target_change, mappings = result
                    if idx % LOG_INTERVAL == 0:
                        if idx != 0:
                            logger.spam(
                                f'Found `{num_mappings_in_interval}` mappings.'
                            )
                        num_mappings_in_interval = 0
                        pos = int(idx / LOG_INTERVAL)
                        low = pos * LOG_INTERVAL
                        high = (pos + 1) * LOG_INTERVAL
                        logger.spam(
                            f'Mapping pairs {low}-{high} of `{num_pairs}`.'
                        )
                    idx += 1
                    for mapping in mappings:
                        num_mappings_in_interval += 1
                        counter[mapping] += 1
                        mapping_source_changes[mapping].add(source_change)
                        mapping_target_changes[mapping].add(target_change)
        else:
            for idx, (source_change, target_change) in enumerate(pairs):
                try:
                    if idx % LOG_INTERVAL == 0:
                        if idx != 0:
                            logger.spam(
                                f'Found `{num_mappings_in_interval}` mappings.'
                            )
                        num_mappings_in_interval = 0
                        pos = int(idx / LOG_INTERVAL)
                        low = pos * LOG_INTERVAL
                        high = (pos + 1) * LOG_INTERVAL
                        logger.spam(
                            f'Mapping pairs {low}-{high} of `{num_pairs}`.'
                        )
                    idx += 1
                    with Timeout(seconds=1):
                        mappings = source_change.map_to_other(target_change)
                except (TypeError, TimeoutError):
                    pass
                else:
                    for mapping in mappings:
                        num_mappings_in_interval += 1
                        counter[mapping] += 1
                        mapping_source_changes[mapping].add(source_change)
                        mapping_target_changes[mapping].add(target_change)

        # Construct an empty graph for computing the change intersection.
        g = nx.Graph()

        # Compute the list of all source arguments used in all mappings.
        source_arguments = {
            key
            for mapping in counter
            for key in mapping.source_arguments
        }

        # Sort all mappings by their frequency.
        sorted_mappings = sorted(
            counter.items(),
            key=lambda item: item[1],
            reverse=True,
        )

        # For each source argument, add the top 20 most frequent mappings that
        # the argument appears in.
        for arg in source_arguments:
            i = 0
            for mapping, count in sorted_mappings:
                if arg in mapping.source_arguments:
                    g.add_node(mapping, weight=count)
                    i += 1
                    if i == 19:
                        break

        # If the empty mapping was generated, add it to the graph. This covers
        # changes that map without arguments.
        empty = ConfigurationTaskArgumentMapping()
        if empty in counter:
            g.add_node(empty, weight=counter[empty])

        # Consider all pairs of mappings that were added to the graph. If the
        # mappings can be merged, then add an edge to the graph indicating that
        # they are compatible.
        logger.debug('Finding compatible mappings.')
        num_pairs = int((len(g.nodes())**2 - len(g.nodes())) / 2)
        for idx, (u, v) in enumerate(combinations(g.nodes(), 2)):
            if idx % LOG_INTERVAL == 0:
                pos = int(idx / LOG_INTERVAL)
                low = pos * LOG_INTERVAL
                high = (pos + 1) * LOG_INTERVAL
                logger.spam(
                    f'Merging pairs {low}-{high} of `{num_pairs}`.'
                )

            # TODO Replicate if u_target != v_target?
            try:
                u.merge(v)
            except MatchingException:
                pass
            else:
                g.add_edge(u, v)
        logger.debug('Done.')

        # Find a maximum weighted clique in g. This clique represents the
        # largest set of mappings (and their associated source and target
        # changes) that are compatible together. The associated changes are
        # the change intersection.
        logger.debug(
            f'Computing clique. '
            f'{len(g.nodes())} nodes, {len(g.edges())} edges.'
        )
        try:
            with Timeout(seconds=30):
                clique, _ = nx.max_weight_clique(g)
        except TimeoutError:
            clique = None
        logger.debug('Done.')

        # If there are no nodes in the clique, return the empty intersection.
        if not clique:
            return set(), set(), ConfigurationTaskArgumentMapping()

        # Unpack all source changes, target changes, and mappings from the
        # list of nodes in the clique. The final mapping is the merging of all
        # individual mappings from the clique (which are known to be compatible
        # based on our construction of g).
        source_changes = set(chain.from_iterable(
            mapping_source_changes[mapping]
            for mapping in clique
        ))
        target_changes = set(chain.from_iterable(
            mapping_target_changes[mapping]
            for mapping in clique
        ))
        mapping = ConfigurationTaskArgumentMapping.merge_all(clique)
        return source_changes, target_changes, mapping

    def __str__(self) -> str:
        """Return a string representation of the change.

        Returns
        -------
        str
            A human readable format for ``self``.
        """
        parts = [
            (
                arg.name,
                type(v)(map(str, v))
                if isinstance(v := getattr(self, arg.name), Iterable) else
                str(v)
            )
            for arg in self.__dataclass_fields__.values()
        ]
        width = max(len(name) for name, _ in parts)
        field_strings = '\n'.join(
            f'    {name:{width}s} - {value}'
            for name, value in parts
        )

        return f'{self.__class__.__name__}:\n' + field_strings


@dataclass(frozen=True)
class DirectoryAdd(ConfigurationChange):
    """A directory was created."""

    path: SyntheticValue


@dataclass(frozen=True)
class DirectoryDelete(ConfigurationChange):
    """A directory was removed."""

    path: SyntheticValue


@dataclass(frozen=True)
class EnvSet(ConfigurationChange):
    """An environment variable was set to a new value."""

    key: SyntheticValue
    value: SyntheticValue


@dataclass(frozen=True)
class EnvUnset(ConfigurationChange):
    """An environment variable was unset."""

    key: SyntheticValue


@dataclass(frozen=True)
class FileAdd(ConfigurationChange):
    """A file was created."""

    path: SyntheticValue


@dataclass(frozen=True)
class FileDelete(ConfigurationChange):
    """A file was removed."""

    path: SyntheticValue


@dataclass(frozen=True)
class FileChange(ConfigurationChange):
    """A text file was changed."""

    path: SyntheticValue
    changes: tuple[FileContentChange, ...]


class FileContentChangeType(str, Enum):
    """A type of file change."""

    ADDITION = 'addition'
    DELETION = 'deletion'


@dataclass(frozen=True)
class FileContentChange(DataclassWithSyntheticValues):
    """A change to text file contents."""

    change_type: FileContentChangeType
    content: SyntheticValue

    def __str__(self) -> str:
        """Return a string representation of the content change.

        Returns
        -------
        str
            A human readable format for ``self``.
        """
        return f'{self.change_type.name} - {self.content.original_value[:100]}'


@dataclass(frozen=True)
class ServiceStart(ConfigurationChange):
    """A long-running service was started."""

    name: SyntheticValue


@dataclass(frozen=True)
class ServiceStop(ConfigurationChange):
    """A long-running service was stopped."""

    name: SyntheticValue


@dataclass(frozen=True)
class SymbolicLink(ConfigurationChange):
    """A symbolic link to another file."""

    path: SyntheticValue
    link: SyntheticValue


@dataclass(frozen=True)
class WorkingDirectorySet(ConfigurationChange):
    """The current working directory was set to a new value."""

    path: SyntheticValue


class frozendict(Mapping):
    """A frozen dictionary type, like frozenset.

    Frozen dicts are hashable, but cannot prevent property changes of contained
    objects, so use with care.
    """

    def __init__(self, *args, **kwargs):
        """Create a new frozen dict."""
        self._hash = None
        self._dict = dict(*args, **kwargs)

    def __contains__(self, item: Any) -> bool:
        """Determine if an item (key) is contained within the dict.

        Parameters
        ----------
        item : Any
            Dict key. Must be hashable.

        Returns
        -------
        bool
            True iff the item is a key of the dict.
        """
        return self._dict.__contains__(item)

    def __len__(self) -> int:
        """Get the length of the dict.

        Returns
        -------
        int
            Number of items contained in the dict.
        """
        return self._dict.__len__()

    def __iter__(self) -> Iterator:
        """Get an iterator for the dict.

        Returns
        -------
        Iterator
            An iterator for dict elements.
        """
        return self._dict.__iter__()

    def __getitem__(self, item: Any) -> Any:
        """Get an item from the dict.

        Parameters
        ----------
        item : Any
            Item key. Must be hashable.

        Returns
        -------
        Any
            The item retrieved by the item key.
        """
        return self._dict.__getitem__(item)

    def __hash__(self) -> int:
        """Hash the dict.

        Returns
        -------
        int
            Dict hash.
        """
        if not self._hash:
            self._hash = hash(tuple(sorted(self.items())))
        return self._hash

    def __repr__(self) -> str:
        """Get a string representation.

        Returns
        -------
        str
            Dict representation.
        """
        return self._dict.__repr__()
