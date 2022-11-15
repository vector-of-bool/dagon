"""
Module ``dagon.option``
#######################

Create run-time execution options to customize task behaviors.

Task can define customization options using this API to tweak task behaviors.
The value assigned to any option is fixed at task graph invocation and is
uniform across a task graph execution.

Using `.option.add` to create new options while declaring tasks, and use
`.option.Option.get` or `.option.value_of` to obtain the value provided to an
option at runtime.

.. data:: OptionType
    :canonical: dagon.option.option.OptionType
    :type: str | bool | pathlib.Path | int | float | enum.Enum

    A type union of any type that can be used with `.Option`.

    .. _option-types:

    Dagon supports several types by default:

    - `str` for strings.
    - `bool` for true/false values.
    - :class:`pathlib.Path` to represent filesystem paths.
    - `int` for integral values.
    - `float` for floating point numbers.
    - Any subclass of :class:`enum.Enum`. It is recommended to use `str` keys
      with enums when using such types.

    In addition, Dagon supports any class that presents a `__dagon_parse_opt__`
    static method, which should receive a string and return an instance of the
    class.

.. data:: OptionT
    :canonical: dagon.option.option.OptionT

    A type variable bound to `.OptionType`.
"""

from __future__ import annotations

import argparse
import enum
import re
import sys
import textwrap
from contextlib import nullcontext
from dataclasses import dataclass, field
from inspect import isclass
from pathlib import Path
from typing import (TYPE_CHECKING, Any, Callable, ContextManager, Generic, Iterable, Mapping, Protocol, Sequence, Type,
                    TypeVar, Union, cast, overload)

import dagon.tool.args

from ..ext.base import BaseExtension
from ..util import DefaultSentinelType, T, U, typecheck

__all__ = ['Option', 'add', 'NoOptionValueError']


class _CustomDagonOpt(Protocol):
    @classmethod
    def __dagon_parse_opt__(cls, val: str) -> _CustomDagonOpt:
        ...


SimpleOptionType = Union[str, bool, Path, int, float, enum.Enum, _CustomDagonOpt]

SimpleT = TypeVar('SimpleT', bound=SimpleOptionType)

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


_DEFAULT_SENTINEL: Any = DefaultSentinelType()


class Option(Generic[T], Protocol):
    """
    Representation of a run-time execution option that can be provided by a
    user.
    """
    @property
    def name(self) -> str:
        ...

    @property
    def doc(self) -> str | None:
        ...

    @property
    def type(self) -> Type[T]:
        ...

    @overload
    def get(self) -> T:
        ...

    @overload
    def get(self, *, default: U) -> T | U:
        ...

    @overload
    def get(self, *, default_factory: Callable[[], U]) -> T | U:
        ...


class _Option(Generic[T]):
    """
    Representation of a run-time execution option that can be provided by a
    user.

    :param name: The name of the option. Should be unique in a given Dag.
    :param typ: The type of the option. Must be a subtype of `.OptionType`.
    :param calc_default: The default value, or a factory function that generates
        the default value. If omitted and no value was provided by a user,
        requesting the option's value will raise an exception.
    :param doc: A documentation string to display to the user.
    :param validate: A secondary validation function that checks that a given
        value is valid for this option. Should return `None` when the value
        is valid, or an error string explaining why it is an invalid value.
    """
    def __init__(self,
                 name: str,
                 *,
                 type: Type[T],
                 parse: Callable[[str], T],
                 calc_default: Callable[[], T] | None = None,
                 doc: str | None = None,
                 validate: Callable[[T], str | None] | None = None) -> None:
        self._name = name
        self._parser = parse
        self._type = type
        self._calc_default = calc_default
        self._validate = validate
        self._doc = doc

    @property
    def name(self) -> str:
        """The option's name"""
        return self._name

    @property
    def doc(self) -> str | None:
        """Documentation for this option"""
        return self._doc

    def get(self, *, default: U = _DEFAULT_SENTINEL, default_factory: Callable[[], U] = _DEFAULT_SENTINEL) -> T | U:
        """
        Obtain the value that was given to this option.

        .. note::
            This method may only be called from within a task-executing context!
        """
        return _ctx_fulfilled_options().get(self, default=default, default_factory=default_factory)

    @property
    def default_factory(self) -> None | Callable[[], T]:
        """Obtain the (optional) default-value factory for this option"""
        return self._calc_default

    def get_default_or_raise(self, default_value: T, default_factory: Callable[[], U]) -> T | U:
        if self._calc_default is not None:
            return self._calc_default()
        if default_value is not _DEFAULT_SENTINEL:
            return default_value
        if default_factory is not _DEFAULT_SENTINEL:
            return default_factory()
        raise NoOptionValueError(f'No value was provided for option "{self.name}", and no default value was specified')

    @property
    def type(self) -> Type[T]:
        return self._type

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


if TYPE_CHECKING:
    typecheck(Option[Any])(_Option[Any])


@overload
def add(name: str,
        type: Type[SimpleT],
        *,
        doc: str = '',
        validate: Callable[[SimpleT], str | None] = ...) -> Option[SimpleT]:
    ...


@overload
def add(name: str,
        type: Type[SimpleT],
        *,
        doc: str = '',
        validate: Callable[[SimpleT], str | None] = ...,
        default: U) -> Option[SimpleT | U]:
    ...


@overload
def add(name: str,
        type: Type[SimpleT],
        *,
        doc: str = '',
        validate: Callable[[SimpleT], str | None] = ...,
        default_factory: Callable[[], U]) -> Option[SimpleT | U]:
    ...


@overload
def add(name: str, type: Type[T] = ..., *, doc: str = '', parse: Callable[[str], T]) -> Option[T]:
    ...


@overload
def add(name: str, type: Type[T] = ..., *, doc: str = '', parse: Callable[[str], T], default: U) -> Option[T | U]:
    ...


@overload
def add(name: str,
        type: Type[T] = ...,
        *,
        doc: str = '',
        parse: Callable[[str], T],
        default_factory: Callable[[], U]) -> Option[T | U]:
    ...


def add(name: str,
        type: Type[T] | None = None,
        *,
        parse: Callable[[str], T] | None = None,
        doc: str = '',
        validate: Callable[[Any], str | None] | None = None,
        default: Any = _DEFAULT_SENTINEL,
        default_factory: Any = _DEFAULT_SENTINEL) -> Option[Any]:
    """
    Add an option to the current task graph with the given name and type.
    """
    oset = _ctx_option_set()
    if type is not None:
        parse = cast(Callable[[str], T], get_type_parser(cast(Type[SimpleOptionType], type)))
    assert parse, 'option.add requires a "parse" function or a supported "type"'
    if default_factory is not _DEFAULT_SENTINEL:
        # The default is a callable
        return oset.add(_Option[Any](name,
                                     type=type,
                                     parse=parse,
                                     doc=doc,
                                     validate=validate,
                                     calc_default=default_factory))
    elif default is not _DEFAULT_SENTINEL:
        return oset.add(_Option[Any](name,
                                     type=type,
                                     parse=parse,
                                     doc=doc,
                                     validate=validate,
                                     calc_default=lambda: default))
    else:
        # No default value provided
        return oset.add(_Option[Any](name, type=type, parse=parse, doc=doc, validate=validate))


class NoOptionValueError(ValueError):
    pass


class _FulfilledOptions:
    """
    Type that holds the result of options that have been fulfilled.

    :param values: Iterable of pairs that define the values for each option.
        The value should match the type of the option, or be ``Undefined``, in
        which case asking for the option value will return ``None``
    """
    def __init__(self, values: Iterable[tuple[_Option[Any], Any]]):
        self._values = dict(values)

    def get(self, option: _Option[T], default: U, default_factory: Callable[[], U]) -> T | U:
        """
        Get the value of the given option.

        If the option exists but no value was provided, returns ``None``. If the
        option does not exist, raises ``KeyError``.
        """
        if not option in self._values:
            raise KeyError(f'No known option "{option.name}"')
        mine = self._values[option]
        if mine is not _DEFAULT_SENTINEL:
            return mine

        fac = option.default_factory
        if fac is not None:
            return fac()
        if default is not _DEFAULT_SENTINEL:
            return default
        if default_factory is not _DEFAULT_SENTINEL:
            return default_factory()
        raise NoOptionValueError(
            f'No value was provided for option "{option.name}", and no default value was specified')


class _OptionSet:
    """
    A collection of options.

    :param options: Initial options to populate with
    """
    def __init__(self, options: Iterable[_Option[Any]] = ()):
        self._options: dict[str, _Option[Any]] = {}
        for o in options:
            self.add(o)

    def add(self, opt: _Option[T]) -> _Option[T]:
        """
        Register the given option with this set. Returns the given option.

        Raises :class:`RuntimeError` if the given option name is already taken.
        """
        if opt.name in self._options:
            raise RuntimeError(f'Duplicate option "{opt.name}"')
        self._options[opt.name] = opt
        return opt

    def get(self, key: str) -> _Option[Any] | None:
        """Get an option by name, or ``None`` if it does not exist in this set."""
        return self._options.get(key)

    @property
    def options(self) -> Iterable[_Option[Any]]:
        """The options in this set"""
        return self._options.values()

    def _iter_fulfill(self, kvs: Iterable[str]) -> Iterable[tuple[_Option[Any], Any]]:
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

        for key in all_keys:
            opt = self.get(key)
            assert opt
            yield opt, _DEFAULT_SENTINEL

    def fulfill(self, kvs: Iterable[str]) -> _FulfilledOptions:
        """
        Fulfill the options in this set using the list of strings, which should
        be of the format ``<key>=<value>``.

        If a given ``key`` does not correpsond to any option, raises
        ``NameError``. If an option was not given a value in ``kvs``, that
        option will return ``None`` in the returned :class:`FulfilledOptions`.
        """
        return _FulfilledOptions(self._iter_fulfill(kvs))


class _OptionsArgs(dagon.tool.args.ParsedArgs, Protocol):
    opts: Sequence[str] | None
    list_options: bool


@dataclass()
class _OptionsContext:
    options: _OptionSet = field(default_factory=_OptionSet)
    fulfilled: None | _FulfilledOptions = None


def _ctx_option_set() -> _OptionSet:
    return _OptionsExt.app_data().options


def _ctx_fulfilled_options() -> _FulfilledOptions:
    c = _OptionsExt.app_data()
    if c.fulfilled is None:
        raise RuntimeError('Options have not yet been fulfilled')
    return c.fulfilled


def _format_docstr(s: str) -> Iterable[str]:
    paragraphs = textwrap.dedent(s).split('\n\n')
    for par in paragraphs:
        for line in textwrap.wrap(par):
            yield line.strip()
        yield ''  # Empty line after paragraph


def _option_help_annot(t: Type[Any]) -> str:
    assert isclass(t), t
    if issubclass(t, enum.Enum):
        return f'{t.__name__} {{' + ', '.join(str(f.value) for f in t) + '}'
    return t.__name__


def _list_options(opts: _OptionSet, with_docs: bool = True) -> int:
    for opt in opts.options:
        print(f'{opt.name}: {_option_help_annot(opt.type)}')
        if with_docs and opt.doc:
            for line in _format_docstr(opt.doc):
                print(f'    {line}')
    return 0


class _OptionsExt(BaseExtension[_OptionsContext, None, None]):
    dagon_ext_name = 'dagon.options'

    def __init__(self) -> None:
        self._set = _OptionSet()

    def add_options(self, arg_parser: argparse.ArgumentParser) -> None:
        grp = arg_parser.add_argument_group('Task Options')
        grp.add_argument('-o',
                         '--opt',
                         dest='opts',
                         metavar='<key>=<value>',
                         action='append',
                         help='Specify the value for a task option (can be repeated)')
        grp.add_argument('--list-options', '-lo', action='store_true', help='List available task options and exit')

    def handle_options(self, opts: argparse.Namespace) -> None:
        args = cast(_OptionsArgs, opts)
        if args.list_options:
            _list_options(_ctx_option_set(), with_docs=not args.no_doc)
            sys.exit(0)
        self.app_data().fulfilled = _ctx_option_set().fulfill(args.opts or ())

    def app_context(self) -> ContextManager[_OptionsContext]:
        return nullcontext(_OptionsContext())
