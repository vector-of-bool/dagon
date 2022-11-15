from datetime import datetime
from pathlib import Path

import pytest

from .. import db as mod


def get_mem_db() -> mod.Database:
    return mod.Database.open(':memory:')


# Mark tests as asyncio to ensure there is a thread loop for the database
@pytest.mark.asyncio
def test_create() -> None:
    mod.open_sqlite_db(':memory:')


@pytest.mark.asyncio
def test_new_run_id() -> None:
    db = get_mem_db()
    run_id = db.new_run_id()
    assert run_id == 1
    run_id_2 = db.new_run_id()
    assert run_id_2 == 2


@pytest.mark.asyncio
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
