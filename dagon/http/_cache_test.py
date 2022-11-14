from datetime import datetime, timedelta
from pathlib import Path
import pytest

from ..util.testing import async_test
from . import _cache as mod


def test_parse_cc():
    cc = mod.CacheControl.parse('no-store, max-age=124')
    assert cc.no_store
    assert not cc.must_revalidate
    assert cc.max_age == 124


def test_mangle_path():
    p = mod.cachepath_for_url('http://google.com', 31)
    assert p == 'http/google.com-nil/c7b9/_-31'

    p = mod.cachepath_for_url('http://google.com/', 8)
    assert p == 'http/google.com-nil/6ab0/_-8'

    p = mod.cachepath_for_url('http://google.com/foo/bar', 0)
    assert p == 'http/google.com-nil/af3e/foo/bar-0'

    p = mod.cachepath_for_url('http://google.com/foo/bar/', 91)
    assert p == 'http/google.com-nil/6f68/foo/bar-91'


@async_test
async def test_save(tmp_path: Path):
    c = mod.open_cache_in(tmp_path)
    f = tmp_path.joinpath('eggs.txt')
    f.write_text('hello, cache!')
    dltime = datetime.now()
    await c.save(url='https://google.com',
                 last_modified=None,
                 etag='meow',
                 cache_control='max-age=30',
                 download_time=dltime,
                 age=12,
                 take_file=f)

    avail = list(c.for_url('https://google.com'))
    assert avail == [
        mod.CacheEntry(
            1,
            'https://google.com',
            'meow',
            mod.NIL_CACHE_CONTROL._replace(max_age=30),
            None,
            12,
            dltime,
        )
    ]
    assert c.path_to(avail[0]).read_text() == 'hello, cache!'
    first = avail[0]
    assert first.age_offset == 12
    assert first.etag == 'meow'
    assert first.expires_at == dltime + timedelta(seconds=30 - 12)
    assert not first.is_stale
