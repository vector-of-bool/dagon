from __future__ import annotations

from typing import Any, Callable, Type, overload

from dagon.util import T, Undefined

from .ext import ctx_fulfilled_options, ctx_option_set
from .option import Option, OptionDefaultArg


def add_option(name: str,
               typ: Type[T],
               *,
               default: OptionDefaultArg[T] = Undefined,
               doc: str = '',
               validate: Callable[[T], str | None] | None = None) -> Option[T]:
    """
    Add an option to the current task graph with the given name and type.
    """
    oset = ctx_option_set()
    return oset.add(Option(name, typ, default=default, doc=doc, validate=validate))


@overload
def value_of(opt: Option[T]) -> T | None:
    ...


@overload
def value_of(opt: str) -> Any:
    ...


def value_of(opt: Option[T] | str) -> T | None:
    """Obtain the value of the specified option"""
    return ctx_fulfilled_options().get(opt)
