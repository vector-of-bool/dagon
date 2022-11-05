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

from typing import Any, Callable, Type, TypeVar, cast, overload
from typing_extensions import Final

from ..util import T, U
from .ext import ctx_fulfilled_options, ctx_option_set
from .option import Option, SimpleOptionType, get_type_parser

__all__ = ['Option', 'add', 'value_of']

SimpleT = TypeVar('SimpleT', bound=SimpleOptionType)


@overload
def add(name: str,
        type: Type[SimpleT],
        *,
        doc: str = '',
        validate: Callable[[SimpleT], str | None] | None = None) -> Option[SimpleT]:
    ...


@overload
def add(name: str,
        type: Type[SimpleT],
        *,
        doc: str = '',
        validate: Callable[[SimpleT], str | None] | None = None,
        default: U) -> Option[SimpleT | U]:
    ...


@overload
def add(name: str,
        type: Type[SimpleT],
        *,
        doc: str = '',
        validate: Callable[[SimpleT], str | None] | None = None,
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


__default_sentinel: Final = object()


def add(name: str,
        type: Type[T] | None = None,
        *,
        parse: Callable[[str], T] | None = None,
        doc: str = '',
        validate: Callable[[Any], str | None] | None = None,
        default: Any = __default_sentinel,
        default_factory: Any = __default_sentinel) -> Option[Any]:
    """
    Add an option to the current task graph with the given name and type.
    """
    oset = ctx_option_set()
    if type is not None:
        parse = cast(Callable[[str], T], get_type_parser(cast(Type[SimpleOptionType], type)))
    assert parse, 'option.add requires a "parse" function or a supported "type"'
    if default_factory is not __default_sentinel:
        # The default is a callable
        return oset.add(Option[Any](name, type=type, parse=parse, doc=doc, validate=validate, calc_default=default))
    elif default is not __default_sentinel:
        return oset.add(Option[Any](name,
                                    type=type,
                                    parse=parse,
                                    doc=doc,
                                    validate=validate,
                                    calc_default=lambda: default))
    else:
        # No default value provided
        return oset.add(Option[Any](name, type=type, parse=parse, doc=doc, validate=validate))


@overload
def value_of(opt: Option[T]) -> T:
    ...


@overload
def value_of(opt: Option[T], *, default: U) -> T | U:
    ...


@overload
def value_of(opt: str, *, default: Any = ...) -> Any:
    ...


def value_of(opt: Option[T] | str, **kw: Any) -> T | Any:
    """Obtain the value of the specified option"""
    return ctx_fulfilled_options().get(opt, **kw)
