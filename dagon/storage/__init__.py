"""
Abstract asynchronous filesystem-like data storage.
"""

from __future__ import annotations

import asyncio
import sqlite3
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path, PosixPath, PurePosixPath
from typing import (IO, TYPE_CHECKING, AsyncContextManager, AsyncIterable, AsyncIterator, Awaitable, Iterable, Optional)

from typing_extensions import Protocol

from .. import fs, util
from .. import db as db_mod
from ..fs import IfExists, NPaths, Pathish, iter_pathish


class IFileWriter(Protocol):
    """
    An object that can be used to write data into storage.
    """
    def write_more(self, _data: bytes) -> Awaitable[None]:
        """
        Append the given data to the stored object.
        """
        ...


class IFileStorage(Protocol):
    """
    Access to filesystem-like storage.
    """
    def open_file_writer(self,
                         fpath: Pathish,
                         *,
                         if_exists: IfExists = 'fail') -> AsyncContextManager[Optional[IFileWriter]]:
        """
        Open a new :class:`~IFileWriter` to write a file into storage. The file
        is stored at the given 'fpath'. Parent "directories" are created
        implicitly.

        :param fpath: The path to open. Will be normalized to POSIX format.
        :param if_exists: The action to take if the given file already exists.

        This is a context manager. When the manager exits, the file is stored
        as-is and can no longer be appended to.

        If ``if_exists`` is `keep`, the context manager yields ``None`` when
        the file exists. Otherwise, the context manager always yields a valid
        :class:`~IFileWriter`.
        """
        ...

    def read_file(self, fpath: Pathish) -> AsyncContextManager[AsyncIterable[bytes]]:
        """
        Open the given file for reading. Returns an async-iterable of data chunks
        of the file.

        :param fpath: The path to open. Will be POSIX-normalized.

        :raises: :class:`FileNotFoundError` if 'fpath' does not exist.
        """
        ...


class _DatabaseFileWriter:
    "Writes a sequence of blobs into a SQLite database."

    def __init__(self, c: sqlite3.Cursor, file_id: int) -> None:
        self._cursor = c
        self._file_id = file_id
        self._counter = 0

    async def write_more(self, data: bytes) -> None:
        self._cursor.execute(
            r'''
            INSERT INTO dagon_storage_file_data (file_id, nth, data)
            VALUES (:file, :n, :data)
            ''',
            dict(file=self._file_id, n=self._counter, data=data),
        )
        self._counter += 1


_META_VERSION = 1


class _DatabaseFileStorage:
    def __init__(self, db: db_mod.Database, subkey: int) -> None:
        self._db = db
        self._subkey = subkey
        self._migrate_dbstore(db)

    @staticmethod
    def _do_migrate_dbstore(db: db_mod.Database) -> None:
        for s in [
                'DROP TABLE IF EXISTS dagon_storage_files',
                'DROP TABLE IF EXISTS dagon_storage_file_data',
                r'''
                CREATE TABLE dagon_storage_files (
                    file_id INTEGER PRIMARY KEY,
                    run_id NOT NULL REFERENCES dagon_runs ON DELETE CASCADE,
                    path TEXT NOT NULL,
                    UNIQUE(run_id, path)
                )''',
                r'''
                CREATE TABLE dagon_storage_file_data (
                    data_id INTEGER PRIMARY KEY,
                    file_id NOT NULL REFERENCES dagon_storage_files ON DELETE CASCADE,
                    nth INTEGER NOT NULL,
                    data BLOB NOT NULL,
                    UNIQUE(file_id, nth)
                )''',
        ]:
            db(s)

    @staticmethod
    def _migrate_dbstore(db: db_mod.Database) -> None:
        db.sqlite3_db.execute(r'''
            CREATE TABLE IF NOT EXISTS dagon_storage_meta (
                meta INTEGER NOT NULL
            )
        ''')
        meta = list(db('SELECT meta FROM dagon_storage_meta'))
        if meta and meta[0][0] == _META_VERSION:
            # Database schema is up-to-date
            return

        with util.recursive_transaction(db.sqlite3_db):
            _DatabaseFileStorage._do_migrate_dbstore(db)
            db('INSERT INTO dagon_storage_meta(meta) VALUES (:ver)', ver=_META_VERSION)

    @asynccontextmanager
    async def read_file(self, fpath: Pathish) -> AsyncIterator[AsyncIterable[bytes]]:
        """Implements IFileStorage.read_file()"""
        fpath = PurePosixPath(fpath)
        fid = list(
            self._db(
                r'''
                SELECT file_id
                  FROM dagon_storage_files
                 WHERE path = :path
                       AND run_id = :rid
                ''',
                path=str(fpath),
                rid=self._subkey,
            ))
        if not fid:
            raise FileNotFoundError(fpath)
        fid = fid[0][0]
        items: Iterable[tuple[bytes]] = self._db(
            r'''
            SELECT data
              FROM dagon_storage_file_data
             WHERE file_id = :fid
             ORDER BY nth
            ''',
            fid=fid,
        )
        yield self._read_items(items)

    async def _read_items(self, items: Iterable[tuple[bytes]]) -> AsyncIterable[bytes]:
        for i, in items:
            yield i

    def open_file_writer(self,
                         fpath: Pathish,
                         *,
                         if_exists: IfExists = 'fail') -> AsyncContextManager[Optional[IFileWriter]]:
        """
        Implements IFileStorage.open_file_writer
        """
        return self._open_file_writer(fpath, if_exists)

    @asynccontextmanager
    async def _open_file_writer(self, fpath: Pathish, if_exists: IfExists) -> AsyncIterator[Optional[IFileWriter]]:
        fpath = PurePosixPath(fpath)
        c = self._db.sqlite3_db.cursor()
        dml_cmd = {
            'fail': 'INSERT',
            'keep': 'INSERT',
            'replace': 'INSERT OR REPLACE',
        }[if_exists]
        if dml_cmd is None:
            raise ValueError(f'Invalid file storage `in_exists` `{if_exists}`')
        if if_exists == 'keep':
            exists: bool = util.first(
                self._db(r'''
                        SELECT count(*) FROM dagon_storage_files
                        WHERE path = :path AND run_id = :subkey
                        ''',
                         path=str(fpath),
                         subkey=self._subkey))[0]
            if exists:
                yield None
                return
        try:
            c.execute(
                fr'''
            {dml_cmd} INTO dagon_storage_files (run_id, path)
            VALUES (:run, :path)
            ''', dict(run=self._subkey, path=str(fpath)))
        except sqlite3.IntegrityError as e:
            if 'UNIQUE constraint failed' in str(e):
                raise FileExistsError(fpath) from e
            raise
        file_id = c.lastrowid
        fw: IFileWriter = _DatabaseFileWriter(c, file_id)
        yield fw


class _NativeFileWriter:
    def __init__(self, of: IO[bytes]):
        self._fd = of

    async def write_more(self, dat: bytes) -> None:
        remain = len(dat)
        while remain:
            remain -= self._fd.write(dat)


class NativeFileStorage:
    def __init__(self, directory: Pathish):
        self._path = Path(directory).resolve()

    @property
    def path(self) -> Path:
        return self._path

    def _normpath(self, given: Pathish) -> Path:
        p = PurePosixPath(given)
        if not p or p.parts[0] == '..':
            raise ValueError(f'Invalid path "{given}"')
        return Path(self.path / p)

    def open_file_writer(self,
                         fpath: Pathish,
                         *,
                         if_exists: IfExists = 'fail') -> AsyncContextManager[IFileWriter | None]:
        return self._open_file_writer(fpath, if_exists)

    @asynccontextmanager
    async def _open_file_writer(self, fpath: Pathish, if_exists: IfExists) -> AsyncIterator[IFileWriter | None]:
        fpath = self._normpath(fpath)

        fpath.parent.mkdir(exist_ok=True, parents=True)
        if if_exists == 'replace':
            with open(fpath, 'wb') as fd:
                yield _NativeFileWriter(fd)
                return
        try:
            fpath.parent.mkdir(exist_ok=True, parents=True)
            with open(fpath, 'xb') as fd:
                yield _NativeFileWriter(fd)
            return
        except FileExistsError:
            if if_exists == 'fail':
                raise
            if if_exists == 'keep':
                yield None
            else:
                assert 0

    @asynccontextmanager
    async def read_file(self, fpath: Pathish) -> AsyncIterator[AsyncIterable[bytes]]:
        fpath = self._normpath(fpath)
        with open(fpath, 'rb') as f:
            yield fs.read_blocks_from(f)


if TYPE_CHECKING:
    util.typecheck(IFileStorage)(_DatabaseFileStorage)
    util.typecheck(IFileWriter)(_DatabaseFileWriter)
    util.typecheck(IFileStorage)(NativeFileStorage)
    util.typecheck(IFileWriter)(_NativeFileWriter)


@asynccontextmanager
async def open_db_storage(*,
                          db: Optional[db_mod.Database] = None,
                          run_id: Optional[int] = None) -> AsyncIterator[IFileStorage]:
    if db is None or run_id is None:
        dbctx = db_mod.global_context_data()
        if not dbctx:
            raise RuntimeError('open_db_storage() requires a database argument or to be called within a task context')
        db = db or dbctx.database
        run_id = run_id if run_id is not None else dbctx.run_id
    async with db.transaction_context():
        yield _DatabaseFileStorage(db, run_id)


@asynccontextmanager
async def _ensure_storage(st: IFileStorage | None) -> AsyncIterator[IFileStorage]:
    if st is not None:
        yield st
        return

    dbctx = db_mod.global_context_data()
    if dbctx is None:
        raise RuntimeError(f'Invalid attempt to use database storage outside of a task context')
    db = dbctx.database
    async with db.transaction_context():
        yield _DatabaseFileStorage(db, dbctx.run_id)


async def store(files: NPaths,
                *,
                whence: Pathish | None = None,
                prefix: PurePosixPath | None = None,
                if_exists: IfExists = 'fail',
                into: IFileStorage | None = None) -> None:
    async with _ensure_storage(into) as into:
        whence = Path(whence or Path.cwd())
        prefix = PurePosixPath(prefix or '.')

        coros = (_store_file(into, f, whence, prefix, if_exists) for f in iter_pathish(files))
        await asyncio.gather(*coros)


async def _store_file(into: IFileStorage, fpath: Path, whence: Path, prefix: PurePosixPath,
                      if_exists: IfExists) -> None:
    fpath = fpath.resolve(strict=False)
    try:
        suffix = fpath.relative_to(whence).as_posix()
    except ValueError as e:
        raise RuntimeError(f'Cannot store file outside of working directory without a "whence" '
                           f'argument set to a parent directory of the file being stored. '
                           f'(filepath is "{fpath}", whence path is "{whence}")') from e
    stored_path = PosixPath(prefix.joinpath(suffix))
    await store_file_as(fpath, stored_path, into=into, if_exists=if_exists)


async def store_file_as(fpath: Pathish,
                        dest: Pathish,
                        *,
                        into: IFileStorage | None = None,
                        if_exists: IfExists = 'fail') -> None:
    async with AsyncExitStack() as stack:
        into = await stack.enter_async_context(_ensure_storage(into))
        writer = await stack.enter_async_context(into.open_file_writer(dest, if_exists=if_exists))
        if writer is None:
            assert if_exists == 'keep', if_exists
            return
        await store_file_in_writer(fpath, writer)


async def store_file_in_writer(fpath: Pathish, writer: IFileWriter) -> None:
    async for block in fs.read_blocks(fpath):
        await writer.write_more(block)


async def recover(fpath: Pathish, *, from_: IFileStorage | None = None) -> bytes:
    acc = b''
    async for part in recover_iter(fpath, from_=from_):
        acc += part
    return acc


async def recover_iter(fpath: Pathish, *, from_: IFileStorage | None = None) -> AsyncIterator[bytes]:
    async with AsyncExitStack() as st:
        from_ = await st.enter_async_context(_ensure_storage(from_))
        bufs = await st.enter_async_context(from_.read_file(fpath))
        async for b in bufs:
            yield b


if TYPE_CHECKING:
    f: IFileStorage = fs
    util.unused(f)
