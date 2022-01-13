"""
Module for the Dagon persistent database
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import datetime
import enum
import hashlib
import json
import os
import sqlite3
from contextlib import ExitStack, asynccontextmanager, nullcontext
from pathlib import Path, PurePath, PurePosixPath
from typing import (Any, AsyncGenerator, AsyncIterator, ContextManager, Iterable, NamedTuple, NewType, Optional,
                    Sequence, Union, cast)
from typing_extensions import Protocol

from dagon.core.result import Cancellation, Failure, NodeResult, Success
from dagon.ext.iface import OpaqueTaskGraphView
from dagon.task.dag import OpaqueTask
from typing_extensions import Literal, TypeAlias

from .. import util
from ..event import events
from ..ext.base import BaseExtension
from ..fs import Pathish
from ..util import Undefined, fixup_dataclass_docs

QueryParameter = Union[int, str, bytes, float, None]
"""Any type that is known-safe to use as a database query parameter"""


class TaskState(enum.Enum):
    """The state of a task"""
    Pending = 'pending'
    'Task is enqueued, but has not started'
    Running = 'running'
    'Task is running'
    Succeeded = 'succeeded'
    'Task executed to completion without error'
    Failed = 'failed'
    'Task raised an exception'
    Cancelled = 'cancelled'
    'Task was cancelled'


class DatabaseError(RuntimeError):
    """
    Exception thrown for database errors
    """


class MissingDatabase(DatabaseError):
    """
    Exception thrown when the requested database file is absent.
    """
    def __init__(self, path: Path) -> None:
        super().__init__(f'Missing database file at "{path}"')
        self.expected_path = path


class InvalidDatabase(DatabaseError):
    """
    Exception thrown if an existing database cannot be ready by Dagon
    """
    def __init__(self, path: Path) -> None:
        super().__init__(f'Not a Dagon database file at "{path}"')
        self.path = path


class IncorrectDatabaseVersion(DatabaseError):
    """
    Exception thrown if the database version mismatches what Dagon expects
    """
    def __init__(self, path: Path, version: str, expected_version: str) -> None:
        super().__init__(f'Database version is unsupported (Expected r{expected_version}, got r{version})')
        self.path = path
        self.version = version
        self.expected_version = expected_version


class _ProcOutputItem(Protocol):
    out: bytes
    kind: Literal['error', 'output']


def _exec_kw(db: Union[sqlite3.Connection, sqlite3.Cursor], stmt: str, **kwargs: QueryParameter) -> sqlite3.Cursor:
    return db.execute(stmt, kwargs)


def _db_connect(db_path: Pathish) -> sqlite3.Connection:
    """Open a SQLite database connection. Sets appropriate database options."""
    db = sqlite3.connect(str(db_path), isolation_level=None)
    db.row_factory = sqlite3.Row
    db.executescript(r'''
        PRAGMA foreign_keys = ON;
        PRAGMA temp_store = MEMORY;
        PRAGMA journal_mode = MEMORY;
        PRAGMA synchronous = OFF;
    ''')
    return db


_DB_SCHEMA = r'''
CREATE TABLE dagon_meta (
    hash TEXT NOT NULL
);
CREATE TABLE dagon_runs (
    run_id INTEGER PRIMARY KEY,
    time REAL NOT NULL
);
CREATE TABLE dagon_tasks (
    task_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE dagon_task_runs (
    task_run_id INTEGER PRIMARY KEY,
    task_id
        NOT NULL REFERENCES dagon_tasks
        ON DELETE CASCADE,
    run_id
        NOT NULL REFERENCES dagon_runs
        ON DELETE CASCADE,
    end_state TEXT DEFAULT 'pending',
    start_time REAL NOT NULL,
    duration REAL,
    UNIQUE(task_id, run_id) ON CONFLICT ABORT
);
CREATE TABLE dagon_task_events (
    task_event_id INTEGER PRIMARY KEY,
    task_run_id
        NOT NULL REFERENCES dagon_task_runs
        ON DELETE CASCADE,
    event NOT NULL,
    time REAL NOT NULL
);
CREATE TABLE dagon_proc_execs (
    proc_exec_id INTEGER PRIMARY KEY,
    task_run_id
        NOT NULL REFERENCES dagon_task_runs
        ON DELETE CASCADE,
    cmd NOT NULL,
    start_cwd TEXT NOT NULL,
    stdout BLOB,
    stderr BLOB,
    retc NOT NULL,
    start_time REAL NOT NULL,
    duration REAL NOT NULL
);
CREATE TABLE dagon_task_deps_rel (
    tree_id INTEGER PRIMARY KEY,
    run_id
        NOT NULL REFERENCES dagon_runs
        ON DELETE CASCADE,
    dependent TEXT NOT NULL,
    depends_on TEXT,
    UNIQUE(run_id, dependent, depends_on) ON CONFLICT REPLACE
);
CREATE TABLE dagon_persists (
    task_id REFERENCES dagon_tasks DEFAULT NULL,
    key TEXT NOT NULL,
    data TEXT NOT NULL
);
CREATE TABLE dagon_intervals (
    interval_id INTEGER PRIMARY KEY,
    task_run_id REFERENCES dagon_task_runs ON DELETE CASCADE,
    label TEXT NOT NULL,
    meta,
    start_time REAL NOT NULL,
    end_time REAL
);
'''

_DB_EXTRAS = r'''
CREATE TEMP VIEW dagon_file_data_sizes AS
    SELECT file_id,
           n,
           length(data) AS size
      FROM file_data;

CREATE TEMP VIEW dagon_file_sizes AS
    SELECT file_id,
           sum(size) AS size
      FROM dagon_file_data_sizes
  GROUP BY file_id;

CREATE TEMP VIEW dagon_sized_files AS
    SELECT * FROM files
             NATURAL JOIN dagon_file_sizes;
'''


def schema_hash() -> str:
    """
    Hash of the database schema. Used to version the database until we someday
    need to do real database migrations.
    """
    return hashlib.sha256(_DB_SCHEMA.encode('utf-8')).hexdigest()


def _create_sqlite_db(db_path: Pathish) -> sqlite3.Connection:
    db = _db_connect(db_path)
    db.executescript(_DB_SCHEMA)
    _exec_kw(db, 'INSERT INTO dagon_meta VALUES (:hash)', hash=schema_hash())
    return db


def create_new_sqlite_db(db_path: Pathish) -> sqlite3.Connection:
    """
    Create a new sqlite3 database at ``db_path``. Will remove an existing file
    if necessary.
    """
    if db_path == ':memory:':
        return _create_sqlite_db(db_path)
    db_path = Path(db_path)
    if db_path.exists():
        db_path.unlink()
    return _create_sqlite_db(db_path)


def _existing_db_schema_hash(db: sqlite3.Connection) -> str:
    try:
        for version, in _exec_kw(db, 'SELECT hash FROM dagon_meta'):
            return cast(str, version)
        return ''
    except sqlite3.OperationalError:
        return ''


def get_ready_sqlite_db(db_path: Pathish) -> sqlite3.Connection:
    """
    Get an sqlite3 connection object that is set up and ready to use to persist
    information.

    If the given DB exists and has a matching schema version, it is used as-is.
    If the DB does not exist, or the DB has the wrong schema version, a new
    database is created at ``db_path``.
    """
    db = _db_connect(db_path)
    db.execute(r'''
        CREATE TABLE IF NOT EXISTS dagon_meta (
            version INTEGER NOT NULL
        );
    ''')
    version = _existing_db_schema_hash(db)
    if version != schema_hash():
        db.close()
        db = create_new_sqlite_db(db_path)

    db.executescript(_DB_EXTRAS)
    return db


def get_existing_sqlite_db(db_path: Pathish) -> sqlite3.Connection:
    """
    Obtain an existing sqlite3 database connection. Raises an exception if the
    file does not exist or has the wrong schema version.
    """
    db_path = Path(db_path)
    if not db_path.is_file():
        raise MissingDatabase(db_path)
    db = _db_connect(db_path)
    items = list(
        _exec_kw(
            db,
            '''
            SELECT name FROM sqlite_master
            WHERE type="table" AND name='dagon_meta' LIMIT 1
            ''',
        ))
    if not items:
        raise InvalidDatabase(db_path)

    version, = util.first(_exec_kw(db, 'SELECT hash FROM dagon_meta'))
    if version != schema_hash():
        raise IncorrectDatabaseVersion(db_path, version, schema_hash())

    db.executescript(_DB_EXTRAS)
    return db


RunID = NewType('RunID', int)
TaskRowID = NewType('TaskRowID', int)
TaskRunID = NewType('TaskRunID', int)
FileID = NewType('FileID', int)
IntervalID = NewType('IntervalID', int)
ProcExecID = NewType('ProcExecID', int)

TaskRunRow: TypeAlias = 'tuple[TaskRunID, TaskRowID, RunID, int, str, float, Optional[float]]'


@dataclasses.dataclass(frozen=True)
class FileInfo:
    """
    Information about a file stored in the database
    """
    #: The Run ID that owns the file
    run_id: RunID
    #: The ID of the file object
    file_id: FileID
    #: The path to the file
    path: PurePosixPath
    #: The size of the file (in bytes). See :func:`pretty_size`
    size: int

    @property
    def pretty_size(self) -> str:
        """
        Return a prettified string representing the size of the file.
        """
        s = self.size
        for x in ('bytes', 'KiB', 'MiB', 'GiB', 'TiB'):
            if s < 1024:
                return f'{s:3.1f} {x}'
            s //= 1024
        raise RuntimeError('Value is too large. Wow.')


class Database:
    """
    Database access for persisting and obtaining data about executions.

    :param db: The SQLite database connection to use. Should have the schema
        already present.
    """
    def __init__(self, db: sqlite3.Connection) -> None:
        self._db = db
        self._transaction_lock = asyncio.Lock()

    @staticmethod
    def get_or_create(db_path: Pathish) -> 'Database':
        """
        Get or create a new database object at the given path (or ``:memory:``)
        """
        # db = create_new_sqlite_db(db_path)
        db = get_ready_sqlite_db(db_path)
        return Database(db)

    @staticmethod
    def get_existing(db_path: Pathish) -> 'Database':
        """
        Get an existing database at the named path.
        """
        db = get_existing_sqlite_db(db_path)
        return Database(db)

    @property
    def sqlite3_db(self) -> sqlite3.Connection:
        return self._db

    def new_run_id(self) -> RunID:
        """
        Create a new Run ID
        """
        c = self._db.cursor()
        _exec_kw(c, 'INSERT INTO dagon_runs (time) VALUES (:time)', time=datetime.datetime.now().timestamp())
        cur = RunID(c.lastrowid)
        return cur

    def __call__(self, q: str, **kwargs: QueryParameter) -> sqlite3.Cursor:
        """
        Execute a database query ``q`` with the given keyword argument bindings.
        """
        return _exec_kw(self._db, q, **kwargs)

    @asynccontextmanager
    async def transaction_context(self) -> AsyncGenerator[None, None]:
        async with self._transaction_lock:
            with util.recursive_transaction(self.sqlite3_db):
                yield

    def get_task_rowid(self, task: str) -> TaskRowID:
        """
        Get the ID of the named task (inserting a new row if it does not
        already exist).
        """
        for tid, in self('SELECT task_id FROM dagon_tasks WHERE name = :name', name=task):
            return TaskRowID(tid)
        c = self._db.cursor()
        _exec_kw(c, 'INSERT INTO dagon_tasks (name) VALUES (:name)', name=task)
        return TaskRowID(c.lastrowid)

    def add_task_run(self,
                     *,
                     run: RunID,
                     task: TaskRowID,
                     state: TaskState = TaskState.Pending,
                     start_time: Optional[datetime.datetime] = None) -> TaskRunID:
        """
        Create a new task run in the database.

        :param run: The run that owns the task run.
        :param task: The ID of the task (taken from :func:`get_task_rowid`)
        :param state: The initial state of the task
        :param start_time: The time at which the task started executing

        :returns: The new DB ID of the task run.
        """
        c = self._db.cursor()
        _exec_kw(
            c,
            'INSERT INTO dagon_task_runs (task_id, run_id, start_time, end_state) '
            'VALUES (:task_id, :run_id, :start_time, :state)',
            task_id=task,
            run_id=run,
            start_time=(start_time or datetime.datetime.now()).timestamp(),
            state=state.value,
        )
        trun_id = c.lastrowid
        return TaskRunID(trun_id)

    def new_interval(self, trun_id: TaskRunID, label: str, time: Optional[datetime.datetime] = None) -> IntervalID:
        """
        Create a new interval.

        :param trun_id: The task run ID, taken from :func:`add_task_run`
        :param label: The label of the interval
        :param time: The start time of the interval. Default is the current time.

        :return: The DB ID of the interval.

        .. note:: Use :func:`set_interval_end` to mark the end time of the
            interval
        """
        time = time or datetime.datetime.now()
        c = self._db.cursor()
        _exec_kw(
            c,
            r'''
            INSERT INTO dagon_intervals (task_run_id, start_time, label, meta)
            VALUES (:trun_id, :time, :label, :meta)
            ''',
            trun_id=trun_id,
            time=time.timestamp(),
            label=label,
            meta='{}',
        )
        return IntervalID(c.lastrowid)

    def set_interval_meta(self, interval: IntervalID, meta: Any) -> None:
        """
        Set a metadata value for the interval.

        :param interval: The DB ID obtained from :func:`new_interval`.
        :param meta: Any SQLite-serializable value to attach to the interval.
        """
        self(
            r'''
            UPDATE dagon_intervals
            SET meta = :meta
            WHERE interval_id = :id
            ''',
            id=interval,
            meta=json.dumps(meta),
        )

    def set_interval_end(self, interval: IntervalID, time: Optional[datetime.datetime] = None) -> None:
        """
        Mark the end time of an interval.

        :param interval: The DB ID obtained from :func:`new_interval`
        :param time: The end time of the interval. Defaults to the current time.
        """
        time = time or datetime.datetime.now()
        self(
            r'''
            UPDATE dagon_intervals
            SET end_time = :time
            WHERE interval_id = :id
            ''',
            time=time.timestamp(),
            id=interval,
        )

    def update_task_state(self,
                          trun_id: TaskRunID,
                          state: TaskState | None = None,
                          duration: datetime.timedelta | None = None) -> None:
        """
        Update the state of a task that has run.

        :param trun_id: The task run ID, obtained from :func:`add_task_run`.
        :param state: The new state. If ``None``, the state will not be changed.
        :param duration: The duration of execution. If ``None``, the duration
            will not be changed.
        """
        if state:
            self('UPDATE dagon_task_runs SET end_state = :state WHERE task_run_id = :id', state=state.value, id=trun_id)
        if duration is not None:
            self('UPDATE dagon_task_runs SET duration = :dur WHERE task_run_id = :id',
                 id=trun_id,
                 dur=duration.total_seconds())

    def store_task_event(self, trun_id: TaskRunID, event: str, time: Optional[datetime.datetime] = None) -> None:
        """
        Store an arbitrary event associated with a task.

        :param trun_id: The task run ID, obtained from :func:`add_task_run`
        :param event: String representing the event.
        :param time: The time of the event. If ``None``, uses the current time.
        """
        self(r'''
            INSERT INTO dagon_task_events (task_run_id, event, time)
            VALUES (:tid, :event, :time)
            ''',
             tid=trun_id,
             event=event,
             time=(time or datetime.datetime.now()).timestamp())

    def store_proc_execution(self, trun_id: TaskRunID, *, cmd: Sequence[str], cwd: PurePath,
                             output: Iterable[_ProcOutputItem] | None, retc: int, start_time: datetime.datetime,
                             duration: float) -> ProcExecID:
        """
        Record the execution of a subprocess.

        :param trun_id: The task run ID, obtained from :func:`add_task_run`
        :param cmd: List of strings as the command line.
        :param cwd: The path to the starting working directory of the process.
        :param stdout: The stdout output from the process.
        :param stderr: The stderr output from the process.
        :param retc: The exit code of the process.
        :param state_time: The time that the process started.
        :param duration: The duration (in seconds) of the subprocess execution.

        :return: The DB ID referencing the process execution.
        """
        q = r'''
            INSERT INTO dagon_proc_execs (
                task_run_id,
                cmd,
                start_cwd,
                stdout,
                stderr,
                retc,
                start_time,
                duration
            ) VALUES (
                :trun_id,
                :cmd,
                :cwd,
                :stdout,
                :stderr,
                :retc,
                :start_time,
                :duration
            )
        '''
        output = output or ()
        rowid = self(q,
                     trun_id=trun_id,
                     cmd=json.dumps(cmd),
                     cwd=str(cwd),
                     stdout=b''.join(o.out for o in output if o.kind == 'output'),
                     stderr=b''.join(o.out for o in output if o.kind == 'error'),
                     retc=retc,
                     start_time=start_time.timestamp(),
                     duration=duration).lastrowid
        return ProcExecID(rowid)

    def iter_files(self, *, run_id: Optional[RunID] = None) -> Iterable[FileInfo]:
        """
        Iterate over every file in the database, optionally restricted to the
        given run.

        :param run_id: If not-``None``, only yields files attached to the given
            run.
        """
        if run_id is None:
            rows = self('SELECT file_id, run_id, path, size FROM dagon_sized_files')
        else:
            rows = self('SELECT file_id, run_id, path, size FROM dagon_sized_files WHERE run_id=:r', r=run_id)

        for fid, rid, path_, size in rows:
            yield FileInfo(run_id=rid, file_id=fid, path=PurePosixPath(path_), size=size)

    def iter_file_data(self, file: Union[FileInfo, FileID]) -> Iterable[bytes]:
        """
        Iterate over the chunks of data in the file stored in the database.

        :param file: The file to read from.
        """
        if isinstance(file, FileInfo):
            file = file.file_id
        for dat, in self('SELECT data FROM file_data WHERE file_id=:fid ORDER BY n', fid=file):
            yield dat

    def add_task_dep(self, *, run_id: RunID, dependent: str, depends_on: Optional[str]) -> None:
        """
        Store information about the dependencies between tasks.

        :param run_id: The run to attach this information to.
        :param dependent: The task that has the dependency.
        :param depends_on: The task which is being depended on.
        """
        self(
            '''
            INSERT INTO dagon_task_deps_rel (run_id, dependent, depends_on)
                 VALUES (:r, :d1, :d2)
            ''',
            r=run_id,
            d1=dependent,
            d2=depends_on,
        )

    def iter_task_deps(self, run_id: RunID) -> Iterable[tuple[str, str]]:
        """
        Iterate over the dependency information for tasks in a given run.
        """
        return cast(Iterable['tuple[str, str]'],
                    self('SELECT dependent, depends_on FROM dagon_task_deps_rel WHERE run_id=:r', r=run_id))

    def load_persist(self, key: str, task_id: Optional[TaskRowID]) -> Any:
        """
        Load a persisted value from the database.

        :param key: The key of the stored value.
        :param task_id: The task that they value is attached to. If ``None``,
            it is considered a "global" persisted value.

        :return: The resulting value, or :data:`Undefined` if there is no value
            stored in the database.
        """
        data = self(
            r'''
            SELECT data
              FROM dagon_persists
              WHERE key IS :key
                    and task_id IS :task_id
            ''',
            key=key,
            task_id=task_id,
        )
        for dat_str, in data:
            return json.loads(dat_str)
        return Undefined

    def set_persist(self, key: str, task_id: Optional[TaskRowID], value: Any) -> None:
        """
        Store a persistent value in the database.

        :param key: The key under which to store the value.
        :param task_id: The task to attach the value to. If ``None``, the
            value is "global" and not attached to a task.
        :param value: The value to store. Must be JSON-serializable.
        """
        n_existing, = util.first(
            self('SELECT count(*) FROM dagon_persists WHERE key IS :key AND task_id IS :tid', key=key, tid=task_id))
        assert n_existing in (0, 1), n_existing
        if n_existing:
            self('''
                 UPDATE dagon_persists
                    SET data = :data
                  WHERE key IS :key
                        AND task_id IS :tid
                 ''',
                 data=json.dumps(value),
                 key=key,
                 tid=task_id)
        else:
            self('''
                 INSERT INTO dagon_persists (key, task_id, data)
                 VALUES (:key, :tid, :data)
                 ''',
                 key=key,
                 tid=task_id,
                 data=json.dumps(value))


@fixup_dataclass_docs
@dataclasses.dataclass()
class _AppContext():
    db_path: Path | None = None


class GlobalContext(NamedTuple):
    database: Database
    run_id: RunID


class TaskContext(NamedTuple):
    task_id: TaskRowID
    task_run_id: TaskRunID
    start_time: datetime.datetime


class _TaskContextPriv(NamedTuple):
    pub: TaskContext
    iv_stack: list[IntervalID]


class _DatabaseExt(BaseExtension[_AppContext, GlobalContext, _TaskContextPriv]):
    dagon_ext_name = 'dagon.db'
    dagon_ext_requires = ['dagon.pools', 'dagon.events']

    def add_options(self, arg_parser: argparse.ArgumentParser) -> None:
        grp = arg_parser.add_argument_group('Task Database Options')
        grp.add_argument('--db-path',
                         metavar='<path>',
                         type=Path,
                         help='Path to the database file to use. Default is ".dagon.db"',
                         default=os.environ.get('DAGON_DATABASE_PATH', '.dagon.db'))

    def handle_options(self, opts: argparse.Namespace) -> None:
        self.app_data().db_path = opts.db_path

    def app_context(self) -> ContextManager[_AppContext]:
        return nullcontext(_AppContext())

    @asynccontextmanager
    async def global_context(self, graph: OpaqueTaskGraphView) -> AsyncIterator[GlobalContext]:
        db = Database.get_or_create(self.app_data().db_path or '.dagon.db')
        rid = db.new_run_id()
        # Yield now to let the graph run
        yield GlobalContext(db, rid)
        # Link up all the dependencies
        for task in graph.all_nodes:
            for dep in graph.dependencies_of(task):
                db.add_task_dep(run_id=rid, dependent=task.name, depends_on=dep.name)

    @asynccontextmanager
    async def task_context(self, task: OpaqueTask) -> AsyncIterator[_TaskContextPriv]:
        db = self.global_data()
        now = datetime.datetime.now()
        tid = db.database.get_task_rowid(task.name)
        trun_id = db.database.add_task_run(run=db.run_id, task=tid, state=TaskState.Running, start_time=now)
        with ExitStack() as st:
            st.enter_context(events['dagon.interval-start'].connect(self._iv_start))
            st.enter_context(events['dagon.interval-end'].connect(self._iv_end))
            yield _TaskContextPriv(TaskContext(tid, trun_id, now), [])

    def _iv_start(self, name: str) -> None:
        iv = self.global_data().database.new_interval(self.task_data().pub.task_run_id, name)
        self.task_data().iv_stack.append(iv)

    def _iv_end(self, _: None) -> None:
        iv = self.task_data().iv_stack.pop()
        self.global_data().database.set_interval_end(iv)

    async def notify_result(self, result: NodeResult[OpaqueTask]) -> None:
        db = self.global_data()
        tctx = self.task_data().pub
        st = TaskState.Pending
        if isinstance(result.result, Cancellation):
            st = TaskState.Cancelled
        elif isinstance(result.result, Failure):
            st = TaskState.Failed
        else:
            assert isinstance(result.result, Success)
            st = TaskState.Succeeded
        db.database.update_task_state(tctx.task_run_id, state=st, duration=datetime.datetime.now() - tctx.start_time)


def global_context_data() -> GlobalContext | None:
    try:
        return _DatabaseExt.global_data()
    except LookupError:
        return None


def task_context_data() -> TaskContext | None:
    try:
        return _DatabaseExt.task_data().pub
    except LookupError:
        return None
