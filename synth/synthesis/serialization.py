"""Synth synthesis serialization support."""


# Imports.
import inspect
from collections.abc import Mapping, Sequence, Set
from enum import Enum
from json import JSONEncoder
from typing import Any, get_args, get_origin, get_type_hints, Union

from synth.synthesis import classes


def from_dict(d: dict) -> Any:
    """Deserialize a Synth object from a JSON dict.

    This function is intended for use as the ``object_hook`` parameter in
    ``json.load`` and related methods.

    Objects are deserialized according to the following rules.

    1. ``Dataclass``. If the object has a ``type`` and ``value`` field, is an
       attribute of the ``classes`` module, and has the ``dataclass``
       attributes, instantiate as a ``dataclass`` by calling
       ``from_primitives`` if it is available or the constructor if it is not.
       Arguments will be taken from ``value`` and converted to the
       corresponding ``dataclass`` field type if the type exists in ``classes``
       or the python builtins and is not ``SyntheticValue``. Synthetic values
       are  implicitly handled by calling ``from_primitives``.
    2. Default: Return the input object unchanged.

    Parameters
    ----------
    d : dict
        Input dictionary to convert.

    Returns
    -------
    Any
        The converted object.
    """
    if ('type' in d
            and 'value' in d
            and (d_type := getattr(classes, d['type']), None) is not None
            and hasattr(d_type, '__dataclass_params__')
            and hasattr(d_type, '__dataclass_fields__')):
        d_value = d['value']
        d_type_hints = get_type_hints(d_type)

        # Convert attributes to the correct type.
        for field_name in d_value:
            if field_name not in d_type_hints:
                continue

            # Get information about the field type.
            field_serialized_value = d_value[field_name]
            field_type = d_type_hints[field_name]
            field_type_origin = get_origin(field_type)
            field_type_args = get_args(field_type)

            # If the field type is a class that is not a synthetic value,
            # construct it directly. Otherwise, if it is a union of types,
            # try to construct it from one of the member types.
            if (inspect.isclass(field_type)
                    and not issubclass(field_type, classes.SyntheticValue)):
                d_value[field_name] = field_type(field_serialized_value)
            elif field_type_origin == Union:
                reconstructed = False
                for arg in field_type_args:
                    arg = get_origin(arg) or arg
                    if ((isinstance(field_serialized_value, dict)
                            and issubclass(arg, Mapping))
                            or (isinstance(field_serialized_value, list)
                                and issubclass(arg, Sequence))
                            or not issubclass(arg, (Mapping, Sequence))):
                        try:
                            d_value[field_name] = arg(field_serialized_value)
                        except TypeError:
                            pass
                        else:
                            reconstructed = True
                            break
                if not reconstructed:
                    raise TypeError(
                        f'Unable to reconstruct `{field_type}` from '
                        f'`{field_serialized_value}`.'
                    )

        # Construct the instance and return.
        if issubclass(d_type, classes.DataclassWithSyntheticValues):
            return d_type.from_primitives(**d_value)
        return d_type(**d_value)
    else:
        return d


class SynthJSONEncoder(JSONEncoder):
    """A JSON encoder for Synth classes."""

    def __init__(self, **kwargs):
        """Create a new JSON encoder.

        All kwargs are forwarded to the JSONEncoder init function, except
        that sort_keys is always set to true.
        """
        kwargs['sort_keys'] = True
        super().__init__(**kwargs)

    def default(self, o: Any) -> Any:
        """Convert objects to a JSON serializable form.

        Objects are converted according to the following rules.

        1. ``SyntheticValue``: The corresponding original value.
        2. ``Dataclass``: A dict containing the object type and value.
        3. ``Enum``: The enum value.
        4. Default: Return the input object unchanged.

        Parameters
        ----------
        o : Any
            An object to convert.

        Returns
        -------
        Any
            An object that is natively JSON serializable.
        """
        if isinstance(o, classes.SyntheticValue):
            return o.original_value
        elif hasattr(o, '__dataclass_fields__'):
            return {
                'type': type(o).__name__,
                'value': {
                    name: getattr(o, name)
                    for name in o.__dataclass_fields__
                }
            }
        elif isinstance(o, Enum):
            return o.value
        elif isinstance(o, Set):
            return sorted(o)
        elif isinstance(o, Mapping):
            return dict(o)
        else:
            return o
