from __future__ import annotations

import contextvars
import types
from contextlib import ExitStack
from typing import (TYPE_CHECKING, Any, AsyncContextManager, Awaitable, Callable, ContextManager, Generator, Generic,
                    Iterable, Type, TypeVar, cast, overload)

from typing_extensions import Protocol

T = TypeVar('T')
T_co = TypeVar('T_co', covariant=True)
U = TypeVar('U')


class UndefinedType:
    _inst: UndefinedType | None = None

    def __new__(cls) -> UndefinedType:
        if cls._inst is None:
            cls._inst = super().__new__(cls)
        return cls._inst


Undefined = UndefinedType()


@overload
def first(items: Iterable[T]) -> T:
    pass


@overload
def first(items: Iterable[T], *, default: U) -> T | U:
    ...


_FirstNoArg = object()


def first(items: Iterable[T], *, default: U | object = _FirstNoArg) -> T | U:
    """
    Return the first element from an iterable object.
    """
    for n in items:
        return n
    if default is _FirstNoArg:
        raise ValueError(f'No first item in an empty iterable ({items!r})')
    return cast(U, default)


def unused(*args: Any) -> None:
    """Does nothing. Used to mark the given arguments as unused."""
    args  # pylint: disable=pointless-statement


class NoneSuch(Generic[T]):
    """
    Represents a missing value of type ``T``, and may have a candidate.

    :param key: The key that was attempted to look up.
    :param candidate: The closest match to ``key``.
    """
    def __init__(self, key: str, candidate: None | T) -> None:
        self._key = key
        self._cand = candidate

    @property
    def key(self) -> str:
        """The key that was invalid"""
        return self._key

    @property
    def candidate(self) -> T | None:
        """The best-matching candidate"""
        return self._cand


class Opaque(Protocol):
    """
    An opaque type. Unlike :class:`Any`, does not implicitly cast anywhere.
    """
    def ___opaque___(self) -> None:
        ...


def kebab_name(name: str) -> str:
    """
    Convert a ``snake_case`` name into a ``kebab-case`` name, and strip
    leading/trailing underscores. Double underscores ``__`` are converted to
    a dot ``.``.

    :param name: The name to convert.

    .. list-table:: Converted Names
        :widths: auto
        :header-rows: 1

        * - Input
          - Result
        * - ``meow``
          - ``meow``
        * - ``john_doe``
          - ``john-doe``
        * - ``do_thing_``
          - ``do-thing``
        * - ``_do_thing``
          - ``do-thing``
        * - ``foo__bar``
          - ``foo.bar``
        * - ``foo__bar_baz``
          - ``foo.bar-baz``
    """
    return name.replace('__', '.').strip('_').replace('_', '-')


def on_context_exit(cb: Callable[[], None]) -> ContextManager[None]:
    st = ExitStack()
    st.callback(cb)
    return cast(ContextManager[None], st)


def scope_set_contextvar(cvar: contextvars.ContextVar[T], value: T) -> ContextManager[None]:
    tok = cvar.set(value)
    return on_context_exit(lambda: cvar.reset(tok))


class ReadyAwaitable(Generic[T]):
    """
    An Awaitable object that when awaited will immediately resolve to a given
    value without suspending the awaiting coroutine.

    :param value: The value that will be returned from the ``await`` expression.
    """
    def __init__(self, value: T):
        self._value = value

    def __await__(self) -> Generator[None, None, T]:
        return self._value
        # Unreachable 'yield', but makes this function into a generator
        yield None


class AsyncNullContext(Generic[T]):
    def __init__(self, value: T = None) -> None:
        self._value = value

    def __aenter__(self) -> Awaitable[T]:
        return ReadyAwaitable(self._value)

    def __aexit__(self, _exc_t: Type[BaseException] | None, _exc: BaseException | None,
                  _tb: types.TracebackType | None) -> Awaitable[None]:
        return ReadyAwaitable(None)


def typecheck(iface: Type[T]) -> Callable[[Type[T]], None]:
    unused(iface)
    assert False, TypeError('typecheck() should never by called at runtime')
    return lambda f: None  # Unreachable, but makes Pylint happy


if TYPE_CHECKING:
    typecheck(AsyncContextManager[None])(AsyncNullContext[None])
    typecheck(AsyncContextManager[int])(AsyncNullContext[int])
