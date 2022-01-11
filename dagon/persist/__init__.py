"""
Module for creating/managing persistent data.
"""

from contextlib import asynccontextmanager, contextmanager
import contextvars
from dagon.ext.iface import OpaqueTaskGraphView
from dagon.task.dag import OpaqueTask
from dagon.util import typecheck, unused
from typing import TYPE_CHECKING, Any, AsyncIterator, Iterator, NamedTuple, Optional, overload
from typing_extensions import Protocol

from ..ext.base import BaseExtension
from .. import db


class IPersistentDataAccess(Protocol):
    def set(self, key: str, value: Any) -> None:
        ...

    @overload
    def get(self, key: str) -> Any:
        ...

    @overload
    def get(self, key: str, default: Any) -> None:
        ...

    def has(self, key: str) -> bool:
        ...


class DatabasePersistor:
    def __init__(self, db_: db.Database, task_id: Optional[db.TaskRowID]) -> None:
        self._db = db_
        self._task = task_id

    def get(self, key: str, default: Any = db.Undefined) -> Any:
        val = self._db.load_persist(key, self._task)
        if val is db.Undefined:
            val = default
        if val is db.Undefined:
            raise KeyError(f'No persisted value for key "{key}"')
        return val

    def set(self, key: str, value: Any) -> None:
        return self._db.set_persist(key, self._task, value)

    def has(self, key: str) -> bool:
        return db.Undefined is not self._db.load_persist(key, self._task)


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

    def get(self, key: str, default: Any = db.Undefined) -> Any:
        return self.cv_.get().get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.cv_.get().set(key, value)

    def has(self, key: str) -> bool:
        return self.cv_.get().has(key)


if TYPE_CHECKING:
    typecheck(IPersistentDataAccess)(DatabasePersistor)
    typecheck(IPersistentDataAccess)(_ImplicitPersistenceItem)


class _PersistExt(BaseExtension[None, None, None]):
    dagon_ext_name = 'dagon.persist'
    dagon_ext_requires = ['dagon.db']

    @asynccontextmanager
    async def global_context(self, graph: OpaqueTaskGraphView) -> AsyncIterator[None]:
        gdb = db.global_context_data()
        assert gdb
        with set_global_persistor(DatabasePersistor(gdb.database, None)):
            yield

    @asynccontextmanager
    async def task_context(self, task: OpaqueTask) -> AsyncIterator[None]:
        gdb = db.global_context_data()
        tdb = db.task_context_data()
        assert tdb and gdb
        with set_task_local_persistor(DatabasePersistor(gdb.database, tdb.task_id)):
            yield


class _ImplicitPersistence(NamedTuple):
    globl: IPersistentDataAccess = _ImplicitPersistenceItem(_GLOBAL_PERSIST)
    local: IPersistentDataAccess = _ImplicitPersistenceItem(_TASK_LOCAL_PERSIST)


_imp_persist = _ImplicitPersistence()

unused(_PersistExt)

globl = _imp_persist.globl
local = _imp_persist.local
