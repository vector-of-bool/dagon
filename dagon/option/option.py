"""
Execution options
"""

from __future__ import annotations

import enum
from pathlib import Path
from typing import (Any, Callable, Generic, Mapping, Type, TypeVar, Union, cast)
from typing_extensions import Protocol

from dagon.util import Undefined, UndefinedType


class _CustomDagonOpt(Protocol):
    @classmethod
    def __dagon_parse_opt__(cls, val: str) -> _CustomDagonOpt:
        ...


OptionType = Union[str, bool, Path, int, float, enum.Enum, _CustomDagonOpt]

OptionT = TypeVar('OptionT', bound=OptionType)


def convert_bool(s: str) -> bool:
    """
    Convert the given string into a bool with some common bool-ish values.
    """
    b = {
        '0': False,
        'no': False,
        'n': False,
        'false': False,
        '1': True,
        'yes': True,
        'y': True,
        'true': True,
    }.get(s.lower())
    if b is None:
        raise ValueError(f'Invalid boolean option string "{s}"')
    return b


_CONVERSION_MAP: Mapping[Type[Any], Callable[[str], Any]] = {
    str: str,
    bool: convert_bool,
    Path: Path,
    int: int,
    float: float,
}

_PARSE_OPT_METHOD_KEY = '__dagon_parse_opt__'


class NoSuchOptionError(NameError):
    pass


def is_valid_option_type(typ: Type[Any]) -> bool:
    """
    Return `True` if the given type is valid for use as a type with :class:`Option`.
    """
    return hasattr(typ, _PARSE_OPT_METHOD_KEY) or issubclass(typ, enum.Enum) or (typ in _CONVERSION_MAP)


def parse_value_str(typ: Type[OptionT], val_str: str) -> OptionT:
    """
    Given a type `typ` and a value string `val_str`, attempt to convert
    that string to the given `typ`. Raises `ValueError` in cases of
    failure.
    """
    try:
        conv = getattr(type, _PARSE_OPT_METHOD_KEY, None)
        if conv is not None:
            assert callable(conv)
            return cast(OptionT, conv(val_str))  # pylint: disable=not-callable
        if issubclass(typ, enum.Enum):
            try:
                return cast(OptionT, typ(val_str))
            except ValueError as e:
                try:
                    i = int(val_str)
                except ValueError:
                    raise e from e
                else:
                    return cast(OptionT, typ(i))
        return cast(OptionT, _CONVERSION_MAP[typ](val_str))
    except Exception as e:
        raise ValueError(f'Failed to parse "{val_str}" as type `{typ.__name__}`') from e


OptionDefaultArg = Union[OptionT, Callable[[], OptionT], UndefinedType]
'The type of the `Option` `default` argument'


class Option(Generic[OptionT]):
    """
    Representation of a run-time execution option that can be provided by a
    user.

    :param name: The name of the option. Should be unique in a given Dag.
    :param typ: The type of the option. Must be a subtype of `.OptionType`.
    :param default: The default value, or a factory function that generates the
        default value. If omitted and no value was provided by a user,
        requesting the option's value will return `None`.
    :param doc: A documentation string to display to the user.
    :param validate: A secondary validation function that checks that a given
        value is valid for this option. Should return `None` when the value
        is valid, or an error string explaining why it is an invalid value.
    """
    def __init__(self,
                 name: str,
                 typ: Type[OptionT],
                 *,
                 default: OptionDefaultArg[OptionT] = Undefined,
                 doc: str | None = None,
                 validate: Callable[[OptionT], str | None] | None = None) -> None:
        if not is_valid_option_type(typ):
            raise TypeError(f'Type {repr(type)} is not a valid option type')
        self._name = name
        self._type = typ
        self._default = default
        self._validate = validate
        self._doc = doc

    @property
    def name(self) -> str:
        """The option's name"""
        return self._name

    @property
    def type(self) -> Type[OptionT]:
        """The type of this option"""
        return self._type

    @property
    def doc(self) -> str | None:
        """Documentation for this option"""
        return self._doc

    def get(self) -> OptionT | None:
        """
        Obtain the value that was given to this option.

        .. note::
            This method may only be called from within a task-executing context!
        """
        from dagon.option.ext import ctx_fulfilled_options
        return ctx_fulfilled_options().get(self)

    def get_default(self) -> OptionT | UndefinedType:
        """
        Get the default value associated with this option, or :data:`Undefined`
        if no default is provided.
        """
        if self._default is Undefined:
            return Undefined

        if callable(self._default):
            return self._default()
        return self._default

    def validate(self, value: OptionT) -> str | None:
        """
        Check if the given value is valid for this option.

        Returns a string representing the error, or `None` if there was
        no error.
        """
        if self._validate is not None:
            return self._validate(value)
        return None

    def __hash__(self) -> int:
        return id(self)

    def __eq__(self, other: Any) -> bool:
        return self is other

    def __repr__(self) -> str:
        return f'<dagon.option.Option "{self.name}">'

    def parse_str(self, val_str: str) -> OptionT:
        """
        Parse the given string into a value for this option. Does not perform
        additional validation.
        """
        return parse_value_str(self.type, val_str)
