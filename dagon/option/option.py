"""
Execution options
"""

from __future__ import annotations

import enum
from pathlib import Path
from typing import (Any, Callable, Generic, Mapping, Type, TypeVar, Union, cast, overload)

from typing_extensions import Protocol

from ..util import T, U, Undefined, UndefinedType


class _CustomDagonOpt(Protocol):
    @classmethod
    def __dagon_parse_opt__(cls, val: str) -> _CustomDagonOpt:
        ...


SimpleOptionType = Union[str, bool, Path, int, float, enum.Enum, _CustomDagonOpt]
SimpleOptionT = TypeVar('SimpleOptionT', bound=SimpleOptionType)


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


def parse_value_str(typ: Type[SimpleOptionT], val_str: str) -> SimpleOptionT:
    """
    Given a type `typ` and a value string `val_str`, attempt to convert
    that string to the given `typ`. Raises `ValueError` in cases of
    failure.
    """
    try:
        conv = getattr(type, _PARSE_OPT_METHOD_KEY, None)
        if conv is not None:
            assert callable(conv)
            return cast(SimpleOptionT, conv(val_str))  # pylint: disable=not-callable
        if issubclass(typ, enum.Enum):
            try:
                return cast(SimpleOptionT, typ(val_str))
            except ValueError as e:
                try:
                    i = int(val_str)
                except ValueError:
                    raise e from e
                else:
                    return cast(SimpleOptionT, typ(i))
        return cast(SimpleOptionT, _CONVERSION_MAP[typ](val_str))
    except Exception as e:
        raise ValueError(f'Failed to parse "{val_str}" as type `{typ.__name__}`') from e


def get_type_parser(typ: Type[SimpleOptionT]) -> Callable[[str], SimpleOptionT]:
    return lambda s: parse_value_str(typ, s)


class Option(Generic[T]):
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
    @overload
    def __init__(self,
                 name: str,
                 *,
                 type: Type[T] | UndefinedType,
                 parse: Callable[[str], T],
                 doc: str | None = None,
                 validate: Callable[[T], str | None] | None = None) -> None:
        ...

    @overload
    def __init__(self,
                 name: str,
                 *,
                 type: Type[T] | UndefinedType,
                 parse: Callable[[str], T],
                 default: T = ...,
                 doc: str | None = None,
                 validate: Callable[[T], str | None] | None = None) -> None:
        ...

    @overload
    def __init__(self,
                 name: str,
                 *,
                 type: Type[T] | UndefinedType,
                 parse: Callable[[str], T],
                 calc_default: Callable[[], T] = ...,
                 doc: str | None = None,
                 validate: Callable[[T], str | None] | None = None) -> None:
        ...

    def __init__(self,
                 name: str,
                 *,
                 type: Type[T] | UndefinedType = Undefined,
                 parse: Callable[[str], T],
                 calc_default: Callable[[], T] | None = None,
                 doc: str | None = None,
                 validate: Callable[[T], str | None] | None = None,
                 **kwargs: Any) -> None:
        self._name = name
        self._parser = parse
        self._type = type
        self._calc_default = calc_default
        self._validate = validate
        self._doc = doc
        self._has_default = 'default' in kwargs
        self._default: T | UndefinedType = kwargs.pop('default', Undefined)

    @property
    def name(self) -> str:
        """The option's name"""
        return self._name

    @property
    def doc(self) -> str | None:
        """Documentation for this option"""
        return self._doc

    @overload
    def get(self) -> T:
        ...

    @overload
    def get(self, *, default: U) -> T | U:
        ...

    def get(self, **kw: Any) -> T:
        """
        Obtain the value that was given to this option.

        .. note::
            This method may only be called from within a task-executing context!
        """
        from dagon.option.ext import ctx_fulfilled_options
        return ctx_fulfilled_options().get(self, **kw)

    @property
    def type(self) -> Type[T] | UndefinedType:
        return self._type

    @property
    def has_default(self):
        """Whether the option has a default value provided"""
        return self._has_default

    def get_default(self) -> T:
        """
        Get the default value associated with this option, or :data:`Undefined`
        if no default is provided.
        """
        if self._calc_default is not None:
            return self._calc_default()
        return cast(T, self._default)

    def validate(self, value: T) -> str | None:
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

    def parse_str(self, val_str: str) -> T:
        """
        Parse the given string into a value for this option. Does not perform
        additional validation.
        """
        return self._parser(val_str)
