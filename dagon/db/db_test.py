import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from .. import db as mod


def get_mem_db() -> mod.Database:
    return mod.Database.get_or_create(':memory:')


def test_create() -> None:
    mod.get_ready_sqlite_db(':memory:')


def test_get_existing() -> None:
    test_db_s = 'foo.db'
    test_db = Path(test_db_s)
    if test_db.exists():
        test_db.unlink()
    with pytest.raises(mod.MissingDatabase):
        mod.get_existing_sqlite_db(test_db)

    conn = sqlite3.connect(test_db_s)
    conn.commit()
    with pytest.raises(mod.InvalidDatabase):
        mod.get_existing_sqlite_db(test_db)

    conn.execute('create table dagon_meta (hash)')
    phony_version = 'bad'
    conn.execute(f'INSERT INTO dagon_meta VALUES (\'{phony_version}\')')
    conn.commit()

    try:
        mod.get_existing_sqlite_db('foo.db')
        assert False, 'Opening bad database did not raise'
    except mod.IncorrectDatabaseVersion as e:
        assert e.path == test_db
        assert e.version == phony_version
        assert e.expected_version == mod.schema_hash()


def test_new_run_id() -> None:
    db = get_mem_db()
    run_id = db.new_run_id()
    assert run_id == 1
    run_id_2 = db.new_run_id()
    assert run_id_2 == 2


def test_persist() -> None:
    db = get_mem_db()
    assert db.load_persist('foo', None) is mod.Undefined
    db.set_persist('foo', None, 12)
    assert db.load_persist('foo', None) == 12


def test_task_events() -> None:
    db = get_mem_db()
    run = db.new_run_id()
    t = db.get_task_rowid('test')
    trun_id = db.add_task_run(run=run, task=t)
    db.store_task_event(trun_id, 'start')


def test_store_proc_exec() -> None:
    db = get_mem_db()
    run = db.new_run_id()
    t = db.get_task_rowid('test')
    trun_id = db.add_task_run(run=run, task=t)
    db.store_proc_execution(
        trun_id,
        cmd=['foo', 'bar'],
        cwd=Path('/nowhere'),
        output=(),
        retc=12,
        start_time=datetime.now(),
        duration=12,
    )
