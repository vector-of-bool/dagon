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

from typing import Any, Callable, Type, overload

from dagon.util import Undefined

from .ext import ctx_fulfilled_options, ctx_option_set
from .option import Option, OptionDefaultArg, OptionT

__all__ = [
    'Option',
    'OptionT',
    'OptionDefaultArg',
    'add',
    'value_of',
]


def add(name: str,
        typ: Type[OptionT],
        *,
        default: OptionDefaultArg[OptionT] = Undefined,
        doc: str = '',
        validate: Callable[[OptionT], str | None] | None = None) -> Option[OptionT]:
    """
    Add an option to the current task graph with the given name and type.
    """
    oset = ctx_option_set()
    return oset.add(Option(name, typ, default=default, doc=doc, validate=validate))


@overload
def value_of(opt: Option[OptionT]) -> OptionT | None:
    ...


@overload
def value_of(opt: str) -> Any:
    ...


def value_of(opt: Option[OptionT] | str) -> OptionT | None:
    """Obtain the value of the specified option"""
    return ctx_fulfilled_options().get(opt)
