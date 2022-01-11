from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncContextManager, AsyncGenerator, AsyncIterator
import pytest

from dagon import storage as mod
from ..db import Database
from ..fs import Pathish
from dagon import fs


async def _read_file(store: mod.IFileStorage, name: Pathish) -> bytes:
    return await mod.recover(name, from_=store)


class CaseSet_IFileStorage:
    @pytest.fixture
    @pytest.mark.asyncio
    async def store(self, tmp_path: Path) -> AsyncGenerator[mod.IFileStorage, None]:
        async with self.do_create_storage(tmp_path) as f:
            yield f

    def do_create_storage(self, tmp_path: Path) -> AsyncContextManager[mod.IFileStorage]:
        raise NotImplementedError

    @pytest.mark.asyncio
    async def test_simple_open(self, store: mod.IFileStorage):
        async with store.open_file_writer('test.txt', if_exists='replace') as wr:
            assert wr
            await wr.write_more(b'foo')
            await wr.write_more(b'bar')

        assert await _read_file(store, 'test.txt') == b'foobar'

    @pytest.mark.asyncio
    async def test_open_existing(self, store: mod.IFileStorage):
        async with store.open_file_writer('test.txt', if_exists='replace') as wr:
            assert wr
            await wr.write_more(b'foo')

        f = store.open_file_writer('test.txt')
        with pytest.raises(FileExistsError):
            async with f:
                pass
        assert await _read_file(store, 'test.txt') == b'foo'

    @pytest.mark.asyncio
    async def test_open_nonexistent(self, store: mod.IFileStorage):
        f = store.read_file("this-file-does-not-exist.txt'")
        with pytest.raises(FileNotFoundError):
            async with f:
                pass

    @pytest.mark.asyncio
    async def test_open_exist_keep(self, store: mod.IFileStorage):
        async with store.open_file_writer('test.txt', if_exists='replace') as wr:
            assert wr
            await wr.write_more(b'foo')
        async with store.open_file_writer('test.txt', if_exists='keep') as wr:
            assert wr is None
        assert await _read_file(store, 'test.txt') == b'foo'

    @pytest.mark.asyncio
    async def test_open_exist_replace(self, store: mod.IFileStorage):
        async with store.open_file_writer('test.txt', if_exists='replace') as wr:
            assert wr
            await wr.write_more(b'foo')
        async with store.open_file_writer('test.txt', if_exists='replace') as wr:
            assert wr
            await wr.write_more(b'bar')
        assert await _read_file(store, 'test.txt') == b'bar'

    @pytest.mark.asyncio
    async def test_open_multiple_simultaneous(self, store: mod.IFileStorage):
        async with store.open_file_writer('a.txt'):
            async with store.open_file_writer('b.txt'):
                pass

    @pytest.mark.asyncio
    async def test_convenience(self, store: mod.IFileStorage):
        await mod.store_file_as(__file__, 'test.py', into=store)
        content = await mod.recover('test.py', from_=store)
        assert content == Path(__file__).read_bytes()


class TestDatabaseFileStorage(CaseSet_IFileStorage):
    def do_create_storage(self, tmp_path: Path) -> AsyncContextManager[mod.IFileStorage]:
        db = Database.get_or_create(':memory:')
        return mod.open_db_storage(db=db, run_id=db.new_run_id())

    def test_create_twice(self):
        db = Database.get_or_create(':memory:')
        mod.open_db_storage(db=db, run_id=1)
        mod.open_db_storage(db=db, run_id=1)


class TestNativeFileStorage(CaseSet_IFileStorage):
    @asynccontextmanager
    async def do_create_storage(self, tmp_path: Path) -> AsyncIterator[mod.IFileStorage]:
        nfs = mod.NativeFileStorage(tmp_path)
        await fs.remove(nfs.path, recurse=True, absent_ok=True)
        yield nfs
