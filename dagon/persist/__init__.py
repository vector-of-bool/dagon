"""
Module for creating/managing persistent data.
"""

import contextvars
import json
from contextlib import asynccontextmanager, contextmanager
from typing import (TYPE_CHECKING, Any, AsyncIterator, Awaitable, Callable, Generic, Iterator, NamedTuple, Optional,
                    Sequence, overload)

from dagon import util
from dagon.ext.iface import OpaqueTaskGraphView
from dagon.task.dag import OpaqueTask
from dagon.util import T, U, typecheck, unused
from typing_extensions import Protocol

from .. import db
from ..ext.base import BaseExtension
from ..cache import Key

_DEFAULT_SENTINEL: Any = object()


class IPersistentDataAccess(Protocol):
    def set(self, key: Key[T], value: T) -> Awaitable[None]:
        ...

    @overload
    def get(self, key: Key[T]) -> Awaitable[T]:
        ...

    @overload
    def get(self, key: Key[T], *, default: U) -> Awaitable[T | U]:
        ...

    @overload
    def get(self, key: Key[T], *, default_factory: Callable[[], U | Awaitable[U]]) -> Awaitable[T | U]:
        ...

    def has(self, key: Key[Any]) -> Awaitable[bool]:
        ...


class _DatabasePersistor:
    def __init__(self, db_: db.Database, task_id: Optional[db.TaskID]) -> None:
        self._db = db_
        self._task = task_id

    def get(self,
            key: Key[T],
            *,
            default: T = _DEFAULT_SENTINEL,
            default_factory: Callable[[], T] = _DEFAULT_SENTINEL) -> Awaitable[T]:
        """
        Load a persisted value from the database.

        :param key: The key of the stored value.
        :param task_id: The task that they value is attached to. If ``None``,
            it is considered a "global" persisted value.

        :return: The persisted value, or the default if one is provided
        :raises: KeyError if no data was persisted with the given key and no
            default was provided to this call.
        """
        return self._get(key, default, default_factory)

    async def _get(self, key: Key[T], default: T, default_factory: Callable[[], T | Awaitable[T]]) -> T:
        data = self._db(
            r'''
            SELECT data
              FROM dagon_persists
             WHERE key IS :key
                   and task_id IS :task_id
            ''',
            key=key.key,
            task_id=self._task,
        )
        for dat_str, in data:
            return json.loads(dat_str)
        if default is not _DEFAULT_SENTINEL:
            return default
        if default_factory is not _DEFAULT_SENTINEL:
            return await util.ensure_awaitable(default_factory())
        raise KeyError(f'No value for "{key.key}" is persisted, and no default was specified')

    async def set(self, key: Key[T], value: T) -> None:
        """
        Store a persistent value in the database.

        :param key: The key under which to store the value.
        :param task_id: The task to attach the value to. If ``None``, the
            value is "global" and not attached to a task.
        :param value: The value to store. Must be JSON-serializable.
        """
        async with self._db.transaction_context():
            if await self.has(key):
                self._db('''
                        UPDATE dagon_persists
                           SET data = :data
                         WHERE key IS :key
                               AND task_id IS :tid
                        ''',
                         data=json.dumps(value),
                         key=key.key,
                         tid=self._task)
            else:
                self._db('''
                         INSERT INTO dagon_persists (key, task_id, data)
                         VALUES (:key, :tid, :data)
                         ''',
                         key=key.key,
                         tid=self._task,
                         data=json.dumps(value))

    def has(self, key: Key[Any]) -> util.ReadyAwaitable[bool]:
        return util.ReadyAwaitable(0 != util.cell(
            self._db('SELECT count(*) FROM dagon_persists WHERE key IS :key AND task_id IS :tid',
                     key=key.key,
                     tid=self._task)))


_TASK_LOCAL_PERSIST = contextvars.ContextVar[IPersistentDataAccess]('_TASK_LOCAL_PERSIST')
_GLOBAL_PERSIST = contextvars.ContextVar[IPersistentDataAccess]('_GLOBAL_PERSIST')


@contextmanager
def set_task_local_persistor(p: IPersistentDataAccess) -> Iterator[IPersistentDataAccess]:
    t = _TASK_LOCAL_PERSIST.set(p)
    try:
        yield p
    finally:
        _TASK_LOCAL_PERSIST.reset(t)


@contextmanager
def set_global_persistor(p: IPersistentDataAccess) -> Iterator[IPersistentDataAccess]:
    t = _GLOBAL_PERSIST.set(p)
    try:
        yield p
    finally:
        _GLOBAL_PERSIST.reset(t)


def get_task_local_persistor() -> IPersistentDataAccess:
    return _TASK_LOCAL_PERSIST.get()


def get_global_persistor() -> IPersistentDataAccess:
    return _GLOBAL_PERSIST.get()


class _ImplicitPersistenceItem(NamedTuple):
    cv_: contextvars.ContextVar[IPersistentDataAccess]

    def get(self, key: Key[Any], *, default: Any = _DEFAULT_SENTINEL, default_factory: Any = _DEFAULT_SENTINEL) -> Any:
        if default is not _DEFAULT_SENTINEL:
            return self.cv_.get().get(key, default=default)
        if default_factory is not _DEFAULT_SENTINEL:
            return self.cv_.get().get(key, default_factory=default_factory)
        return self.cv_.get().get(key)

    async def set(self, key: Key[T], value: T) -> None:
        await self.cv_.get().set(key, value)

    def has(self, key: Key[Any]) -> Awaitable[bool]:
        return self.cv_.get().has(key)


if TYPE_CHECKING:
    typecheck(IPersistentDataAccess)(_DatabasePersistor)
    typecheck(IPersistentDataAccess)(_ImplicitPersistenceItem)

_MIGRATIONS: Sequence[str] = [
    r'''
    CREATE TABLE dagon_persists (
        task_id REFERENCES dagon_tasks DEFAULT NULL,
        key TEXT NOT NULL,
        data TEXT NOT NULL
    )
    ''',
]


class _PersistExt(BaseExtension[None, None, None]):
    dagon_ext_name = 'dagon.persist'
    dagon_ext_requires = ['dagon.db']

    @asynccontextmanager
    async def global_context(self, graph: OpaqueTaskGraphView) -> AsyncIterator[None]:
        gdb = db.global_context_data()
        assert gdb
        with set_global_persistor(in_database(gdb.database)):
            yield

    @asynccontextmanager
    async def task_context(self, task: OpaqueTask) -> AsyncIterator[None]:
        gdb = db.global_context_data()
        tdb = db.task_context_data()
        assert tdb and gdb
        with set_task_local_persistor(in_database(gdb.database, task=tdb.task_id)):
            yield


class _ImplicitPersistence(NamedTuple):
    globl: IPersistentDataAccess = _ImplicitPersistenceItem(_GLOBAL_PERSIST)
    local: IPersistentDataAccess = _ImplicitPersistenceItem(_TASK_LOCAL_PERSIST)


_imp_persist = _ImplicitPersistence()

unused(_PersistExt)

globl = _imp_persist.globl
local = _imp_persist.local


def in_database(db_: db.Database, *, task: db.TaskID | None = None) -> IPersistentDataAccess:
    db.apply_migrations(db_.sqlite3_db, 'dagon_persist_meta', _MIGRATIONS)
    return _DatabasePersistor(db_, task)
