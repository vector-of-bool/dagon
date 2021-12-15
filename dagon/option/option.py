"""
Execution options
"""

from __future__ import annotations

import enum
import re
from pathlib import Path
from typing import (Any, Callable, Generic, Iterable, Mapping, Optional, Type, TypeVar, Union, cast)

from dagon.util import Undefined, UndefinedType, first

T = TypeVar('T')


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
    Return ``True`` if the given type is valid for use as a type with :class:`Option`.
    """
    return hasattr(typ, _PARSE_OPT_METHOD_KEY) or issubclass(typ, enum.Enum) or (typ in _CONVERSION_MAP)


def parse_value_str(typ: Type[T], val_str: str) -> T:
    """
    Given a type ``typ`` and a value string ``val_str``, attempt to convert
    that string to the given ``typ``. Raises ``ValueError`` in cases of
    failure.
    """
    try:
        conv = getattr(type, _PARSE_OPT_METHOD_KEY, None)
        if conv is not None:
            assert callable(conv)
            return cast(T, conv(val_str))  # pylint: disable=not-callable
        if issubclass(typ, enum.Enum):
            try:
                return cast(T, typ(val_str))
            except ValueError as e:
                try:
                    i = int(val_str)
                except ValueError:
                    raise e from e
                else:
                    return cast(T, typ(i))
        return cast(T, _CONVERSION_MAP[typ](val_str))
    except Exception as e:
        raise ValueError(f'Failed to parse "{val_str}" as type `{typ.__name__}`') from e


#: The type of the :class:`Option` ``default`` argument
OptionDefaultArg = Union[T, Callable[[], T], UndefinedType]


class Option(Generic[T]):
    """
    Representation of a run-time execution option that can be provided by a
    user.

    :param name: The name of the option. Should be unique in a given Dag.
    :param typ: The type of the option. Must meet certain requirements (See
        below).
    :param default: The default value, or a function that generates the default
        value. If omitted, requesting the option's value will return ``None``.
    :param doc: A documentation string to display to the user.
    :param validate: A secondary validation function that checks that a given
        value is valid for this option. Should return ``None`` when the value
        is valid, or an error string explaining why it is an invalid value.

    .. _option-types:

    Dagon supports several types by default:

    - ``str`` for strings.
    - ``bool`` for true/false values.
    - :class:`pathlib.Path` to represent filesystem paths.
    - ``int`` for integral values.
    - ``float`` for floating point numbers.
    - Any subclass of :class:`enum.Enum`. It is recommended to use ``str`` keys
      with enums when using this type.

    In addition, Dagon supports any class that presents a ``__dagon_parse_opt__``
    static method, which should receive a string and return an instance of the
    class.
    """
    def __init__(self,
                 name: str,
                 typ: Type[T],
                 *,
                 default: OptionDefaultArg[T] = Undefined,
                 doc: Optional[str] = None,
                 validate: Optional[Callable[[T], Optional[str]]] = None) -> None:
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
    def type(self) -> Type[T]:
        """The type of this option"""
        return self._type

    @property
    def doc(self) -> Optional[str]:
        """Documentation for this option"""
        return self._doc

    def get_default(self) -> Union[T, UndefinedType]:
        """
        Get the default value associated with this option, or :data:`Undefined`
        if no default is provided.
        """
        if self._default is Undefined:
            return Undefined

        if callable(self._default):
            return self._default()
        return self._default

    def validate(self, value: T) -> Optional[str]:
        """
        Check if the given value is valid for this option.

        Returns a string representing the error, or ``None`` if there was
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
        return parse_value_str(self.type, val_str)


class FulfilledOptions:
    """
    Type that holds the result of options that have been fulfilled.

    :param values: Iterable of pairs that define the values for each option.
        The value should match the type of the option, or be ``Undefined``, in
        which case asking for the option value will return ``None``
    """
    def __init__(self, values: Iterable[tuple[Option[Any], Any]]):
        self._values = dict(values)

    def get(self, option: Union[Option[T], str]) -> Optional[T]:
        """
        Get the value of the given option.

        If the option exists but no value was provided, returns ``None``. If the
        option does not exist, raises ``KeyError``.
        """
        if isinstance(option, str):
            found = first((o for o in self._values.keys() if o.name == option), default=None)
            if found is None:
                raise KeyError(f'No option with name "{option}"')
            option = found
        mine = self._values.get(option)
        if mine is None:
            raise KeyError(f'No known option "{option.name}"')
        if mine is Undefined:
            return None
        return cast(T, mine)


class OptionSet:
    """
    A collection of options.

    :param options: Initial options to populate with
    """
    def __init__(self, options: Iterable[Option[Any]] = ()):
        self._options: dict[str, Option[Any]] = {}
        for o in options:
            self.add(o)

    def add(self, opt: Option[T]) -> Option[T]:
        """
        Register the given option with this set. Returns the given option.

        Raises :class:`RuntimeError` if the given option name is already taken.
        """
        if opt.name in self._options:
            raise RuntimeError(f'Duplicate option "{opt.name}"')
        self._options[opt.name] = opt
        return opt

    def get(self, key: str) -> Optional[Option[Any]]:
        """Get an option by name, or ``None`` if it does not exist in this set."""
        return self._options.get(key)

    @property
    def options(self) -> Iterable[Option[Any]]:
        """The options in this set"""
        return self._options.values()

    def _iter_fulfill(self, kvs: Iterable[str]) -> Iterable[tuple[Option[Any], Any]]:
        opt_re = re.compile(r'(.+?)=(.*)')
        all_keys = set(self._options.keys())
        for spec in kvs:
            mat = opt_re.match(spec)
            if not mat:
                raise ValueError(f'Invalid option specifier "{spec}" (should bey <key>=<value>)')
            key, val_str = mat.groups()
            opt = self.get(key)
            if opt is None:
                raise NoSuchOptionError(key)
            try:
                value = opt.parse_str(val_str)
            except Exception as e:
                raise ValueError(f'Failed to parse given string "{val_str}" as a value for option "{opt.name}"') from e
            err = opt.validate(value)
            if err is not None:
                raise RuntimeError(f'Invalid value for `{key}`: {err}')
            yield opt, value
            all_keys.remove(opt.name)

        for k in all_keys:
            opt = self._options[k]
            yield opt, opt.get_default()

    def fulfill(self, kvs: Iterable[str]) -> FulfilledOptions:
        """
        Fulfill the options in this set using the list of strings, which should
        be of the format ``<key>=<value>``.

        If a given ``key`` does not correpsond to any option, raises
        ``NameError``. If an option was not given a value in ``kvs``, that
        option will return ``None`` in the returned :class:`FulfilledOptions`.
        """
        return FulfilledOptions(self._iter_fulfill(kvs))
