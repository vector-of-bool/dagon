from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Iterable, NamedTuple, cast
from urllib.parse import quote as url_quote
from urllib.parse import urlparse

from .. import db as db_mod
from .. import fs
from ..util.paths import user_cache_path


class CacheAccess:
    def __init__(self, files_root: Path, db: db_mod.Database) -> None:
        self._files_root = files_root
        self._db = db

    @property
    def root(self) -> Path:
        """The root directory of all cached data"""
        return self._files_root

    def for_url(self, url: str) -> Iterable[CacheEntry]:
        q = cast(
            Iterable[tuple[int, str, str | None, str | None, str | None, int | None, int]],
            self._db(
                r'''
            SELECT resource_id,
                   url,
                   etag,
                   cache_control,
                   last_modified,
                   age,
                   download_time
              FROM dagon_http_cache
             WHERE url = :url
            ''',
                url=url,
            ))
        for res_id, u, etag, cache_control, last_mod, age, dl_time in q:
            yield CacheEntry(res_id, u, etag,
                             CacheControl.parse(cache_control) if cache_control else NIL_CACHE_CONTROL, last_mod, age,
                             datetime.fromtimestamp(dl_time))

    async def drop(self, entry: CacheEntry) -> None:
        async with self._db.transaction_context():
            fpath = self.path_to(entry)
            await fs.remove(fpath, absent_ok=True)
            self._db(r'DELETE FROM dagon_http_cache WERE resource_id=:id', id=entry.resource_id)

    async def save(self, *, url: str, last_modified: str | None, etag: str | None, cache_control: str | None,
                   age: int | None, download_time: datetime, take_file: Path) -> None:
        if (last_modified, etag, cache_control) == (None, None, None):
            raise TypeError('Insufficient arguments to perform caching (Need at least '
                            'one of last_modified, etag, or cache_control)')
        async with self._db.transaction_context():
            c = self._db(
                r'''
                INSERT INTO dagon_http_cache
                    (url, etag, cache_control, last_modified, age, download_time)
                VALUES
                    (:url, :etag, :cc, :lmod, :age, :dtime)
                ''',
                url=url,
                etag=etag,
                cc=cache_control,
                lmod=last_modified,
                age=age,
                dtime=download_time.timestamp(),
            )
            rowid = c.lastrowid
            assert rowid
            cachepath = cachepath_for_url(url, rowid)
            dest = self._files_root / cachepath
            await fs.safe_move_file(take_file, dest, if_exists='replace', mkdirs=True)

    def path_to(self, entry: CacheEntry) -> Path:
        return self._files_root / cachepath_for_url(entry.url, entry.resource_id)


def cachepath_for_url(url: str, rowid: int) -> str:
    urltup = urlparse(url)
    safepath = mangle_path(urltup.path)
    x = hashlib.md5(url.encode()).hexdigest()[:4]
    return str(PurePosixPath(f'{urltup.scheme}/{urltup.hostname}-{urltup.port or "nil"}/{x}/{safepath}-{rowid}'))


def mangle_path(p: str) -> PurePosixPath:
    parts = PurePosixPath(p).parts
    if parts and parts[0] == '/':
        parts = parts[1:]
    if len(parts) == 0:
        return PurePosixPath('_')
    par = PurePosixPath(*(mangle_path_part(p) for p in parts[:-1]))
    return par / mangle_path_part(parts[-1])


def mangle_path_part(p: str) -> str:
    q = url_quote(p)
    if len(q) > 8:
        q = q[:8]
    return q


class CacheEntry(NamedTuple):
    resource_id: int
    url: str
    etag: str | None
    cache_control: CacheControl
    last_modified: str | None
    age_offset: int | None
    download_time: datetime

    @property
    def age(self) -> timedelta:
        since_req = datetime.now() - self.download_time
        return since_req + timedelta(seconds=self.age_offset or 0)

    @property
    def has_max_age(self) -> bool:
        return self.cache_control.max_age is not None

    @property
    def is_fresh(self) -> bool:
        """Whether the cache entry is "fresh" according to its Cache-Control headers"""
        return (self.cache_control.max_age is not None) and (self.age.total_seconds() < self.cache_control.max_age)

    @property
    def is_stale(self) -> bool:
        """Whether the entry is expired at the current time"""
        return (self.cache_control.max_age is not None) and (self.age.total_seconds() >= self.cache_control.max_age)

    @property
    def expires_at(self) -> datetime | None:
        """The datetime at which this cache entry will be considered expired"""
        if self.cache_control.max_age is None:
            return None
        return self.download_time + timedelta(seconds=self.cache_control.max_age) - timedelta(
            seconds=self.age_offset or 0)


def open_user_cache() -> CacheAccess:
    http_dir: Path = user_cache_path() / 'dagon/http'
    return open_cache_in(http_dir)


def open_cache_in(dirpath: Path) -> CacheAccess:
    dirpath.mkdir(exist_ok=True, parents=True)
    db = db_mod.Database.open(dirpath / 'data.db')
    db_mod.apply_migrations(db.sqlite3_db, 'dagon_http_meta', [
        r'''
        CREATE TABLE dagon_http_cache (
            resource_id INTEGER PRIMARY KEY,
            url TEXT NOT NULL,
            etag TEXT,
            cache_control TEXT,
            last_modified TEXT,
            age INTEGER,
            download_time INTEGER NOT NULL,
            CONSTRAINT has_cache_attr CHECK (
               (etag NOT NULL) +
               (cache_control NOT NULL) +
               (last_modified NOT NULL) > 0
            )
        )
        ''',
        r'CREATE INDEX idx_dagon_http_cache_by_etag ON dagon_http_cache (etag)',
        r'CREATE INDEX idx_dagon_http_cache_by_url ON dagon_http_cache(url)',
    ])
    return CacheAccess(dirpath / 'files', db)


class CacheControl(NamedTuple):
    private: bool
    public: bool
    no_store: bool
    no_cache: bool
    immutable: bool
    max_age: int | None
    must_revalidate: bool

    @staticmethod
    def parse(hdr: str) -> CacheControl:
        items = hdr.split(',')
        items = [s.strip().lower() for s in items]
        max_age: int | None = None
        prefix = 'max-age='
        for i in items:
            if not i.startswith(prefix):
                continue
            tail = i[len(prefix):]
            try:
                max_age = int(tail)
            except ValueError:
                pass
            break
        return CacheControl(private='private' in items,
                            public='public' in items,
                            no_store='no-store' in items,
                            no_cache='no-cache' in items,
                            max_age=max_age,
                            immutable='immutable' in items,
                            must_revalidate='must-revalidate' in items)


NIL_CACHE_CONTROL = CacheControl(False, False, False, False, False, None, False)
