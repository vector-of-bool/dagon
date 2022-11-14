"""
Module ``dagon.util``
#####################

General utilities
"""
from __future__ import annotations

import contextvars
import difflib
import sqlite3
import types
from contextlib import ExitStack, contextmanager
from typing import (TYPE_CHECKING, Any, AsyncContextManager, Awaitable, Callable, ContextManager, Generator, Generic,
                    Iterable, Iterator, Mapping, MutableMapping, MutableSequence, Sequence, Type, TypeVar, Union, cast,
                    overload)

from typing_extensions import Protocol

T = TypeVar('T')
"A generic invariant type variable"
T_co = TypeVar('T_co', covariant=True)
"A generic covariant type variable"
U = TypeVar('U')
"A second generic invariant type variable"


class DefaultSentinelType():
    pass


_DEFAULT_SENTINEL: Any = DefaultSentinelType()


@overload
def first(items: Iterable[T]) -> T:
    ...


@overload
def first(items: Iterable[T], *, default: U) -> T | U:
    ...


def first(items: Iterable[T], default: U = _DEFAULT_SENTINEL) -> T | U:
    """
    Obtain the first element of an iterable

    :param items: An iterable object
    :param default: A default value to return in case of an empty iterable.

    :raises ValueError: If `items` is empty and no `default` was provided
        omitted, `ValueError` will be raised in case of an empty iterable.
    """
    for n in items:
        return n
    if default is not _DEFAULT_SENTINEL:
        return default
    raise ValueError(f'No first item in an empty iterable ({items!r})')


@overload
def cell(table: Iterable[Iterable[T]]) -> T:
    ...


@overload
def cell(table: Iterable[Iterable[T]], *, default: U) -> T | U:
    ...


def cell(table: Iterable[Iterable[T]], *, default: U = _DEFAULT_SENTINEL) -> T | U:
    v = first(first(table, default=(default, )), default=default)
    if v is _DEFAULT_SENTINEL:
        raise ValueError(f'No first item in an empty iterable ({table!r})')
    return v


def unused(*args: Any) -> None:
    """Does nothing. Used to mark the given arguments as unused."""
    _ = args


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

    def __repr__(self) -> str:
        return f'<NoneSuch given="{self.key}" nearest={self.candidate!r}>'


class Opaque(Protocol):
    """
    An opaque type. Unlike :class:`Any`, does not implicitly cast anywhere.
    """
    def ___opaque___(self) -> None:
        ...


def dot_kebab_name(name: str) -> str:
    """
    Convert a ``snake_case`` name into a ``dot.kebab-case`` name, and strip
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
    """Create a context manager that simply calls the given callback on ``__exit__``"""
    st = ExitStack()
    st.callback(cb)
    return cast(ContextManager[None], st)


def scope_set_contextvar(cvar: contextvars.ContextVar[T], value: T) -> ContextManager[None]:
    """
    Create a context manager that sets the
    `context variable <contextvars.ContextVar>` to the given value, then resets
    the value on ``__exit__``
    """
    tok = cvar.set(value)
    return on_context_exit(lambda: cvar.reset(tok))


def typecheck(iface: Type[T]) -> Callable[[Type[T]], Type[T]]:
    """
    Given a type, return a callable that accepts that type. This can be used
    to insert type checks into modules. Should not be called at runtime: guard
    this with a `typing.TYPE_CHECKING` condition.
    """
    unused(iface)
    assert False, TypeError('typecheck() should never by called at runtime')
    return lambda f: f  # Unreachable, but makes Pylint happy


def typecheckv(iface: Type[T]) -> Callable[[T], T]:
    """
    Given a type, return a callable that accepts **a value** of that typte. This
    can be used to insert type checks into modules. Should not be called at
    runtime: guard this with a `typing.TYPE_CHECKING` condition.
    """
    unused(iface)
    assert False, TypeError('typecheckv() should never by called at runtime')
    return lambda f: f  # Unreachable, but makes Pylint happy


class ReadyAwaitable(Generic[T]):
    """
    An `Awaitable` object that when awaited will immediately resolve to a given
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
    """
    Like `~contextlib.nullcontext`, but implements an `~typing.AsyncContextManager`
    """
    def __init__(self, value: T = None) -> None:
        self._value = value

    async def __aenter__(self) -> T:
        return self._value

    async def __aexit__(self, _exc_t: Type[BaseException] | None, _exc: BaseException | None,
                        _tb: types.TracebackType | None) -> None:
        return None


if TYPE_CHECKING:
    typecheck(AsyncContextManager[None])(AsyncNullContext[None])
    typecheck(AsyncContextManager[int])(AsyncNullContext[int])


def nearest_matching(given: str, of: Iterable[T], key: Callable[[T], str]) -> T | None:
    """
    Find the object that is "nearest" to the `given` string based on the string
    distance. Each object should be mapped to a string with the `key` function.

    If `of` is empty, returns `None`.
    """
    return max(of, key=lambda t: difflib.SequenceMatcher(None, given, key(t)).ratio(), default=None)


def fixup_dataclass_docs(cls: Type[T]) -> Type[T]:
    cls.__init__.__qualname__ = f'{cls.__name__}.__init__'
    return cls


def ensure_awaitable(val: Awaitable[T] | T) -> Awaitable[T]:
    """
    Take an object that may or may not be `~typing.Awaitable` at runtime, and
    ensure that it is awaitable. If the given object is not awaitable, it will
    be wrapped in a `.ReadyAwaitable`.
    """
    if isinstance(val, Awaitable):
        return cast(Awaitable[T], val)
    return ReadyAwaitable(val)


@contextmanager
def recursive_transaction(db: sqlite3.Connection) -> Iterator[None]:
    """
    Create a scoped "recursive" transaction on the given SQLite database
    connection.

    If the database is already in a transaction, this context manager is a no-op
    on entry and exit. Otherwise, creates a transaction that is COMMITed on
    exit, or ROLLedBACK if the scope exists via exception.

    asserts that no one closed the transaction outside of our watch.
    """
    if db.in_transaction:
        try:
            yield
        finally:
            assert db.in_transaction, 'transaction was ended prematurely'
        return
    db.execute('BEGIN')
    try:
        yield
    except:
        assert db.in_transaction, 'transaction was ended prematurely'
        db.execute('ROLLBACK')
        raise
    else:
        assert db.in_transaction, 'transaction was ended prematurely'
        db.execute('COMMIT')


JSONScalar = Union[int, float, bool, None, str]
JSONArray = Sequence['JSONValue']
JSONObject = Mapping[str, 'JSONValue']
JSONValue = Union[JSONScalar, JSONArray, JSONObject]

MutableJSONArray = MutableSequence['MutableJSONValue']
MutableJSONObject = MutableMapping[str, 'MutableJSONValue']
MutableJSONValue = Union[JSONScalar, JSONArray, JSONObject]


class _LazyAttrLookup(Generic[T]):
    def __init__(self, callback: Callable[[], T]) -> None:
        self.__callback__ = callback

    def __getattribute__(self, __name: str) -> Any:
        cb = object.__getattribute__(self, '__callback__')
        this = cb()
        return getattr(this, __name)


def create_lazy_lookup(cb: Callable[[], T]) -> T:
    return cast(T, _LazyAttrLookup(cb))
