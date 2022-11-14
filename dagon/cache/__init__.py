from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import (TYPE_CHECKING, Any, AsyncIterator, Generic, NamedTuple, NewType, TypeVar, cast, overload)

from typing_extensions import Final, Literal, Protocol, Self

from dagon import util

from .. import db
from ..ext.base import BaseExtension
from ..ext.iface import OpaqueTaskGraphView
from ..task.dag import OpaqueTask
from ..util import JSONValue, T, U, create_lazy_lookup, typecheck

JSONValueT = TypeVar('JSONValueT', bound=JSONValue)

_DEFAULT_SENTINEL: Any = object()
_db_mod = db


class Key(Generic[T]):
    def __init__(self, key: str) -> None:
        self.key = key


class _GlobalContext(NamedTuple):
    db: _db_mod.Database
    tmp_db: _db_mod.Database
    run_cache: _DbCache
    cache: _DbCache


class _TaskContext(NamedTuple):
    run_cache: _DbCache
    cache: _DbCache


_CacheNodeID = NewType('_CacheNodeID', int)
_CacheDirID = NewType('_CacheDirID', int)

_MIGRATIONS: list[str] = [
    r'''
    CREATE TABLE dagon_cache_nodes (
        node_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        parent_id INTEGER REFERENCES dagon_cache_nodes(node_id)
            ON DELETE CASCADE,
        CONSTRAINT dagon_cache_node_uniqueness UNIQUE (name, parent_id)
    )
    ''',
    r'''
    CREATE TABLE dagon_cache_entries (
        entry_id INTEGER PRIMARY KEY,
        node_id INTEGER
            NOT NULL
            REFERENCES dagon_cache_nodes(node_id)
                ON DELETE CASCADE,
        key TEXT NOT NULL,
        value TEXT NOT NULL,
        CONSTRAINT dagon_cache_entry_uniqueness
            UNIQUE(key, node_id)
    )
    ''',
    r'''
    CREATE TABLE dagon_cache_dirs (
        dir_id INTEGER PRIMARY KEY,
        node_id INTEGER
            NOT NULL
            REFERENCES dagon_cache_nodes,
        parent_id INTEGER REFERENCES dagon_cache_dirs(dir_id),
        name TEXT NOT NULL,
        UNIQUE (parent_id, name)
    )
    ''',
    r'''
    CREATE TABLE dagon_cache_files (
        file_id INTEGER PRIMARY KEY,
        dir_id INTEGER
            NOT NULL
            REFERENCES dagon_cache_dirs(dir_id),
        name TEXT NOT NULL,
        UNIQUE (dir_id, name)
    )
    ''',
]


class _Ext(BaseExtension[None, _GlobalContext, _TaskContext]):
    dagon_ext_name = 'dagon.cache'
    dagon_ext_requires = ['dagon.db']

    @asynccontextmanager
    async def global_context(self, graph: OpaqueTaskGraphView) -> AsyncIterator[_GlobalContext]:
        gdb = db.global_context_data()
        assert gdb
        gdb = gdb.database
        persistent_id = _DbCache.prepare_db(gdb, None)
        tmpdb = db.Database.open('')
        tmp_id = _DbCache.prepare_db(tmpdb, None)
        yield _GlobalContext(
            gdb,  #
            tmpdb,
            _DbCache(gdb, _CacheNodeID(tmp_id)),
            _DbCache(gdb, _CacheNodeID(persistent_id)))

    @asynccontextmanager
    async def task_context(self, task: OpaqueTask) -> AsyncIterator[_TaskContext]:
        globl = self.global_data()
        gdb = globl.db
        tmpdb = globl.tmp_db
        persistent_id = _DbCache.prepare_db(gdb, task.name)
        tmp_id = _DbCache.prepare_db(tmpdb, task.name)
        yield _TaskContext(
            _DbCache(tmpdb, _CacheNodeID(tmp_id)),  #
            _DbCache(gdb, _CacheNodeID(persistent_id)))


class ICache(Protocol):
    @overload
    def child(self, key: str) -> Self:
        ...

    @overload
    def child(self, key: str, *, if_missing: Literal['create', 'fail']) -> Self:
        ...

    @overload
    def child(self, key: str, *, if_missing: Literal['ignore']) -> Self | None:
        ...

    def remove_child(self, key: str, *, absent_ok: bool = False) -> None:
        ...

    def set(self, key: Key[JSONValueT], value: JSONValueT) -> JSONValueT:
        ...

    @overload
    def pop(self, key: Key[JSONValueT]) -> JSONValueT:
        ...

    @overload
    def pop(self, key: Key[JSONValueT], *, default: U) -> JSONValueT | U:
        ...

    @overload
    def get(self, key: Key[JSONValueT]) -> JSONValueT:
        ...

    @overload
    def get(self, key: Key[JSONValueT], *, default: U) -> JSONValueT | U:
        ...


class _DbCache(ICache):
    def __init__(self, db_: db.Database, node_id: _CacheNodeID) -> None:
        self._node = node_id
        self._db = db_

    @staticmethod
    def prepare_db(db_: db.Database, task_: str | None) -> int:
        db.apply_migrations(db_.sqlite3_db, 'dagon_cache_meta', _MIGRATIONS)
        # Create the root for graphs, and a root for tasks
        db_(r'''
            INSERT OR IGNORE INTO dagon_cache_nodes (node_id, name)
            VALUES (0, '@graph'), (1, '@task')
            ''')
        if task_ is None:
            return 0
        db_(
            r'''
            INSERT OR IGNORE INTO dagon_cache_nodes (name, parent_id)
            VALUES (:task, 1)
            ''',
            task=task_,
        )
        task_id = util.cell(
            db_(
                r'''
                SELECT node_id FROM dagon_cache_nodes
                WHERE name=:task AND parent_id=1
                ''',
                task=task_,
            ))
        return task_id

    def child(self, key: str, *, if_missing: Literal['create', 'fail', 'ignore'] = 'fail') -> _DbCache | None:
        if if_missing == 'create':
            self._db(
                r'''
                INSERT OR IGNORE INTO dagon_cache_nodes
                    (name, parent_id)
                VALUES (:name, :parent)
                ''',
                name=key,
                parent=self._node,
            )
        child_id: int | None = util.cell(
            self._db(
                r'''
                SELECT node_id
                  FROM dagon_cache_nodes
                 WHERE name=:name AND parent_id=:parent
                ''',
                name=key,
                parent=self._node,
            ),
            default=None,
        )
        if child_id is None:
            if if_missing == 'fail':
                raise LookupError(key)
            return None
        return _DbCache(self._db, _CacheNodeID(child_id))

    def remove_child(self, key: str, *, absent_ok: bool = False) -> None:
        before = self._db.sqlite3_db.total_changes
        self._db(
            r'''
            DELETE FROM dagon_cache_nodes
             WHERE name=:name AND parent_id=:parent
            ''',
            name=f'.{key}',
            parent=self._node,
        )
        after = self._db.sqlite3_db.total_changes
        if before == after and not absent_ok:
            raise LookupError(key)

    def set(self, key: Key[JSONValueT], value: JSONValueT) -> JSONValueT:
        encoded = json.dumps(value, sort_keys=True)
        self._db(
            r'''
            INSERT OR REPLACE INTO dagon_cache_entries
                (node_id, key, value)
            VALUES (:node, :key, :val)
            ''',
            node=self._node,
            key=key.key,
            val=encoded,
        )
        return value

    def _get(self, key: str) -> None | tuple[int, str]:
        return util.first(
            items=self._db(
                r'''
                SELECT entry_id, value FROM dagon_cache_entries
                WHERE node_id=:node AND key=:key
                ''',
                node=self._node,
                key=key,
            ),
            default=None,
        )

    def get(self, key: Key[JSONValueT], *, default: JSONValueT = _DEFAULT_SENTINEL) -> JSONValueT:
        pair = self._get(key.key)
        if pair is None:
            if default is _DEFAULT_SENTINEL:
                raise KeyError(key.key)
            return default
        v = json.loads(str(pair[1]))
        return v

    def pop(self, key: Key[JSONValueT], *, default: JSONValueT = _DEFAULT_SENTINEL) -> JSONValueT:
        pair = self._get(key.key)
        if pair is None:
            if default is _DEFAULT_SENTINEL:
                raise KeyError(key.key)
            return default
        self._db('DELETE FROM dagon_cache_entries WHERE entry_id=:id', id=pair[0])
        v = json.loads(str(pair[1]))
        return v


if TYPE_CHECKING:
    typecheck(ICache)(_DbCache)

task_run: Final[ICache] = create_lazy_lookup(lambda: _Ext.task_data().run_cache)
graph_run: Final[ICache] = create_lazy_lookup(lambda: _Ext.global_data().run_cache)
task: Final[ICache] = create_lazy_lookup(lambda: _Ext.task_data().cache)
graph: Final[ICache] = create_lazy_lookup(lambda: _Ext.global_data().cache)
