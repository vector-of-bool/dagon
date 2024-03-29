from datetime import datetime, timedelta
import json
from pathlib import Path

import pytest

from dagon import task

from .. import util
from .. import http
from tests.test_util import dag_test

JSON_PLACEHOLDER = 'https://jsonplaceholder.typicode.com'

requires_http = pytest.mark.skipif(not http.is_supported(), reason='Requires [http] extra')


@pytest.mark.asyncio
@requires_http
async def test_chunked_get() -> None:
    buf_iter = http.get_chunked(f'{JSON_PLACEHOLDER}/posts')
    buf_acc = b''
    async for buf in buf_iter:
        buf_acc += buf

    data = json.loads(buf_acc)
    assert isinstance(data, list)


@pytest.mark.asyncio
@requires_http
async def test_download() -> None:
    local_file = Path(__file__).parent / 'test-download.json'
    try:
        await http.download(f'{JSON_PLACEHOLDER}/posts', destination=local_file)
        data = json.loads(local_file.read_text())
        assert isinstance(data, list)
    finally:
        if local_file.exists():
            local_file.unlink()


@pytest.mark.asyncio
@requires_http
async def test_download_tmp() -> None:
    async with http.download_tmp(f'{JSON_PLACEHOLDER}/posts/1') as tmpfile:
        json_str = tmpfile.read_text()
        print(json_str)
        data = json.loads(json_str)
        assert isinstance(data, dict)


@dag_test()
@requires_http
def test_cached_downloads():
    url = 'https://github.com/Kitware/CMake/releases/download/v3.25.0-rc4/cmake-3.25.0-rc4-linux-x86_64.tar.gz'

    dur = timedelta()

    @task.define()
    async def download_file():
        nonlocal dur
        start = datetime.now()
        async with http.download_tmp(url):
            pass
        dur = datetime.now() - start

    @task.define(depends=[download_file])
    async def download_again():
        start = datetime.now()
        async with http.download_tmp(url):
            pass
        new_dur = datetime.now() - start
        assert new_dur < dur + timedelta(seconds=2)

    return [download_again]


@dag_test()
@requires_http
def test_download_task(tmp_path: Path):
    url = 'https://github.com/Kitware/CMake/releases/download/v3.25.0-rc4/cmake-3.25.0-rc4-linux-x86_64.tar.gz'

    get_cmake_str = http.download_task('get-str', url)
    get_cmake_fac = http.download_task('get-fac', lambda: url)
    get_cmake_afac = http.download_task('get-afac', lambda: util.ReadyAwaitable(url))
    get_cmake_newname = http.download_task('get-with-filename', url, filename='cmake.tgz')
    get_cmake_newdest = http.download_task('get-with-dest', url, destination=tmp_path / 'cmake.tar.gz')

    @task.define(depends=[get_cmake_str, get_cmake_fac, get_cmake_afac, get_cmake_newname, get_cmake_newdest])
    async def check():
        assert (await task.result_of(get_cmake_str)).name == 'cmake-3.25.0-rc4-linux-x86_64.tar.gz'
        assert (await task.result_of(get_cmake_fac)).name == 'cmake-3.25.0-rc4-linux-x86_64.tar.gz'
        assert (await task.result_of(get_cmake_afac)).name == 'cmake-3.25.0-rc4-linux-x86_64.tar.gz'
        assert (await task.result_of(get_cmake_newname)).name == 'cmake.tgz'
        assert (await task.result_of(get_cmake_newdest)).name == 'cmake.tar.gz'

    return [check]
