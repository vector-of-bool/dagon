from __future__ import annotations

from datetime import datetime
from io import BufferedReader
import tempfile
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import (IO, TYPE_CHECKING, AsyncContextManager, AsyncIterable, AsyncIterator, BinaryIO, Mapping, NamedTuple,
                    Sequence, Union)

import pkg_resources
from dagon import util

from ..util import AsyncNullContext
from .. import fs
from ..event import Handler
from ..event.cancel import CancellationToken, raise_if_cancelled
from ..ext.base import BaseExtension
from ..ext.iface import OpaqueTaskGraphView
from . import _cache

try:
    import aiohttp
except ModuleNotFoundError as e_:
    if e_.name != 'aiohttp':
        raise
    if not TYPE_CHECKING:
        aiohttp = None


class DownloadProgress(NamedTuple):
    n_bytes: int
    total_bytes: int | None
    progress: float | None


def is_supported() -> bool:
    try:
        pkg_resources.require('dagon[http]')
        return True
    except pkg_resources.DistributionNotFound:
        return False


def check_supported() -> None:
    try:
        ws = pkg_resources.WorkingSet()
        ws.require('dagon[http]')
    except pkg_resources.DistributionNotFound as e:
        raise RuntimeError('Install Dagon with the `http` extra to use the '
                           'HTTP functionality (Install as `dagon[http]`)') from e


@dataclass
class _GlobalContext:
    session: aiohttp.ClientSession
    cache: _cache.CacheAccess


@asynccontextmanager
async def _new_context() -> AsyncIterator[_GlobalContext]:
    async with aiohttp.ClientSession() as s:
        yield _GlobalContext(s, _cache.open_user_cache())


@asynccontextmanager
async def _get_context() -> AsyncIterator[_GlobalContext]:
    check_supported()
    try:
        glb = _Ext.global_data()
    except LookupError:
        async with _new_context() as c:
            yield c
    else:
        assert glb
        yield glb


class _Ext(BaseExtension[None, _GlobalContext | None, None]):
    dagon_ext_name: str = 'dagon.http'

    def global_context(self, graph: OpaqueTaskGraphView) -> AsyncContextManager[_GlobalContext | None]:
        if aiohttp is None:
            return AsyncNullContext(None)
        return _new_context()


_LowRequestBodyItem = Union[bytes, AsyncIterable[bytes], BinaryIO, None]


@asynccontextmanager
async def _init_request(
    method: str,
    url: str,
    headers: Mapping[str, str] | None,
    data: _LowRequestBodyItem,
) -> AsyncIterator[aiohttp.ClientResponse]:
    check_supported()
    headers = headers or {}
    async with _get_context() as glb:
        async with glb.session.request(method, url, data=data, headers=headers) as res:
            res.raise_for_status()
            yield res


class _CacheLookupResult(NamedTuple):
    use: _cache.CacheEntry | None
    revalidate: Sequence[_cache.CacheEntry]


class _RequestContext(NamedTuple):
    cache: _cache.CacheAccess
    url: str
    headers: Mapping[str, str]
    cancel: CancellationToken | None
    on_progress: Handler[DownloadProgress] | None


def _try_find_cached(cache: _cache.CacheAccess, url: str) -> _CacheLookupResult:
    all_for_url = cache.for_url(url)
    revalidate: list[_cache.CacheEntry] = []
    for c in all_for_url:
        if c.cache_control.must_revalidate or c.is_stale:
            revalidate.append(c)
        elif c.is_fresh or c.cache_control.immutable:
            # Cache entry is fresh and lacks must-revalidate, so it can always be used
            return _CacheLookupResult(c, [])
        elif c.etag or c.last_modified:
            # Cache entry is not fresh and not stale, so it lacks a max-age. We can still validate
            # using Etag or modification time, though
            revalidate.append(c)
        else:
            # No max-age, no Etag, and no Last-Modified. This cache entry is useless.
            pass
    return _CacheLookupResult(None, revalidate)


async def _get_chunked_with_revalidate(req: _RequestContext,
                                       cached_revalidate: Sequence[_cache.CacheEntry]) -> AsyncIterator[bytes]:
    headers = dict(req.headers)
    if len(cached_revalidate) == 1 and cached_revalidate[0].last_modified:
        headers['If-Modified-Since'] = cached_revalidate[0].last_modified
    else:
        headers['If-None-Match'] = ', '.join(c.etag for c in cached_revalidate if c.etag)
    async with AsyncExitStack() as stack:
        resp = await stack.enter_async_context(_init_request('GET', req.url, headers, None))
        if resp.status == 304:
            resp_etag = resp.headers.get('etag', None)
            # Cache hit!
            chosen: _cache.CacheEntry | None = None
            if resp_etag:
                chosen = util.first((c for c in cached_revalidate if c.etag == resp_etag), default=None)
            if not chosen:
                chosen = util.first(cached_revalidate, default=None)
            if chosen:
                return await _get_chunked_yield_cached(chosen, req)
            # Didn't find anything?
            raise RuntimeError(f'Server responded with "304 Not Modified", but we do '
                               f'not have a cached response [url={req.url}]')
        else:
            return _yield_resp(resp, req, stack.pop_all())


async def _yield_resp(resp: aiohttp.ClientResponse, req: _RequestContext,
                      stack: AsyncExitStack) -> AsyncIterator[bytes]:
    async with stack:
        raise_if_cancelled(req.cancel)
        dltime = datetime.now()
        cl = resp.headers.get('content-length', None)
        total_bytes: int | None
        try:
            total_bytes = int(cl) if cl else None
        except ValueError:
            total_bytes = None
        n_bytes = 0
        do_cache = True
        etag = resp.headers.get('Etag')
        cc = resp.headers.get('Cache-Control')
        lmod = resp.headers.get('Last-Modified')
        try:
            age = int(resp.headers.get('Age', '0'))
        except ValueError:
            age = 0
        if (etag, cc, lmod) == (None, None, None):
            do_cache = False
        tf: IO[bytes] | None = None
        tname = None
        if do_cache:
            req.cache.root.mkdir(exist_ok=True, parents=True)
            tfd, tname = tempfile.mkstemp('.dl', dir=req.cache.root, text=False)
            tf = stack.enter_context(open(tfd, 'wb'))
            stack.push_async_callback(lambda: fs.remove(tname, absent_ok=True))
        async for dat in resp.content.iter_chunked(1024 * 1024):
            raise_if_cancelled(req.cancel)
            n_bytes += len(dat)
            if req.on_progress:
                progress = None if total_bytes is None else (n_bytes / total_bytes)
                req.on_progress(DownloadProgress(n_bytes, total_bytes, progress))
            if tf:
                tf.write(dat)
            yield dat
        if tf:
            assert do_cache
            tf.close()
            assert tname
            await req.cache.save(url=req.url,
                                 last_modified=resp.headers.get('Last-Modified'),
                                 etag=resp.headers.get('Etag'),
                                 age=age,
                                 cache_control=resp.headers.get('Cache-Control'),
                                 download_time=dltime,
                                 take_file=Path(tname))


async def _get_chunked_try_cached(req: _RequestContext) -> AsyncIterator[bytes]:
    cached = _try_find_cached(req.cache, req.url)
    if cached.use:
        return await _get_chunked_yield_cached(cached.use, req)
    return await _get_chunked_with_revalidate(req, cached.revalidate)


async def _get_chunked_yield_cached(cached: _cache.CacheEntry, req: _RequestContext) -> AsyncIterator[bytes]:
    fpath = req.cache.path_to(cached)
    try:
        fd = fpath.open('rb')
        return _agen_read(fd)
    except FileNotFoundError:
        await req.cache.drop(cached)
        return await _get_chunked_try_cached(req)


async def _agen_read(fd: BufferedReader) -> AsyncIterator[memoryview]:
    with fd:
        async for dat in fs.read_blocks_from(fd):
            yield dat


async def get_chunked(url: str,
                      *,
                      headers: Mapping[str, str] | None = None,
                      cancel: CancellationToken | None = None,
                      on_progress: Handler[DownloadProgress] | None = None) -> AsyncIterator[bytes | memoryview]:
    """
    Asynchronously download byte chunks from the remote URL.

    :param url: The URL to download from
    :param cancel: A cancellation token to cancel the operation
    :param on_progress: Optional event to dispatch progress

    Use this function as an asynchronous iterator::

        async for buf in dagon.http.get_chunked('http://example.com/big-file'):
            process_bytes(buf)
    """
    cancel = cancel or CancellationToken.get_context_local()
    gen: AsyncIterator[bytes | memoryview]
    async with _get_context() as ctx:
        req = _RequestContext(ctx.cache, url, headers or {}, cancel, on_progress)
        gen = await _get_chunked_try_cached(req)
        async for part in gen:
            yield part


@asynccontextmanager
async def download_tmp(url: str,
                       *,
                       cancel: CancellationToken | None = None,
                       on_progress: Handler[DownloadProgress] | None = None,
                       delete: bool = True,
                       tmp_dir: fs.Pathish | None = None,
                       suffix: str | None = None) -> AsyncIterator[Path]:
    """
    Download the given URL and store the content in a temporary file, yielding
    the path and (optionally) deleting it when done.

    :param url: The URL to download from
    :param cancel: A cancellation token to cancel the operation
    :param progress: Optional event to dispatch progress
    :param delete: If ``True``, the file will be deleted when the context
        manager exits.
    :param tmp_dir: Override the directory that will contain the temporary file.
        Will use the system's default temporary file directory otherwise.
    :param suffix: Set the filename suffix for the generated temporary file.

    :rtype: AsyncContextManager[Path]

    Use this as an asynchronous context manager::

        async with dagon.http.download_tmp(big_file_url) as local_fpath:
            process_file(local_fpath)
    """

    with tempfile.NamedTemporaryFile('wb', delete=False, dir=None if tmp_dir is None else str(tmp_dir),
                                     suffix=suffix) as tmpfd:
        try:
            async for dat in get_chunked(url, cancel=cancel, on_progress=on_progress):
                tmpfd.write(dat)
        except:
            tmpfd.close()
            if not delete:
                Path(tmpfd.name).unlink()
            raise
        else:
            tmpfd.close()
            yield Path(tmpfd.name)
        finally:
            if delete and Path(tmpfd.name).exists():
                Path(tmpfd.name).unlink()


async def download(url: str,
                   *,
                   destination: fs.Pathish,
                   if_exists: fs.IfExists = 'fail',
                   on_progress: Handler[DownloadProgress] | None = None,
                   cancel: CancellationToken | None = None,
                   use_parent_as_tmp_dir: bool = False) -> Path:
    """
    Download the given URL to the given file destination.

    :param url: The URL to download
    :param destination: The file path where to store the downloaded file
    :param if_exists: The policy of behavior when the file already exists
    :param cancel: A cancellation token to cancel the operation
    :param progress: Optional event to dispatch progress
    :param use_parent_as_tmp_dir: If ``True``, the destination directory will
        be used as the temporary directory during the download.

    .. note::
        If ``if_exists`` is set to `"keep"` and the destination
        already exists, this function will return immediately without
        performing any HTTP operations. This makes it useful to perform "lazy"
        downloads without additional branching and logic.
    """
    destination = Path(destination)
    if destination.exists():
        if if_exists == 'fail':
            raise RuntimeError(f'Download destination {destination} already exists')
        if if_exists == 'keep':
            return destination
        if if_exists == 'replace':
            await fs.remove(destination, cancel=cancel, recurse=True)
    dest = Path(destination)
    tmp_dir = None
    if use_parent_as_tmp_dir:
        tmp_dir = str(dest.parent)

    async with download_tmp(
            url,
            on_progress=on_progress,
            cancel=cancel,
            delete=False,
            tmp_dir=tmp_dir,
    ) as tmp_path:
        dest.parent.mkdir(parents=True, exist_ok=True)
        await fs.safe_move_file(tmp_path, dest, mkdirs=True, cancel=cancel)
    return dest
