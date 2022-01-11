import random

import pytest

from .. import db as db_mod
from .. import persist as mod
from ..task.task_test import run_test_on_fun


@pytest.fixture
def db() -> db_mod.Database:
    return db_mod.Database.get_or_create(':memory:')


def test_persist_global(db: db_mod.Database) -> None:
    with mod.set_global_persistor(mod.DatabasePersistor(db, None)):
        assert not mod.globl.has('eggs')
        mod.globl.set('eggs', 22)
        assert mod.globl.get('eggs') == 22

    with mod.set_global_persistor(mod.DatabasePersistor(db, None)):
        # Reopen the same persistor
        assert mod.globl.has('eggs')
        assert mod.globl.get('eggs') == 22

        # Modify it again
        mod.globl.set('eggs', 42)
        assert mod.globl.get('eggs') == 42

    with mod.set_global_persistor(mod.DatabasePersistor(db, None)):
        assert mod.globl.get('eggs') == 42


def test_persist_local(db: db_mod.Database) -> None:
    tid1 = db.get_task_rowid('example1')
    tid2 = db.get_task_rowid('example2')

    with mod.set_task_local_persistor(mod.DatabasePersistor(db, tid1)):
        assert not mod.local.has('eggs')
        mod.local.set('eggs', 12)
        assert mod.local.get('eggs') == 12

    with mod.set_task_local_persistor(mod.DatabasePersistor(db, tid2)):
        assert not mod.local.has('eggs')
        mod.local.set('eggs', 72)
        assert mod.local.get('eggs') == 72

    with mod.set_task_local_persistor(mod.DatabasePersistor(db, tid1)):
        # Restored from the prior task ID
        assert mod.local.has('eggs')
        assert mod.local.get('eggs') == 12


def test_task_persist_global() -> None:
    val = random.randint(0, 99999)

    async def run() -> None:
        mod.globl.set('test_value', val)

    async def run2() -> None:
        assert mod.globl.get('test_value') == val

    run_test_on_fun(run)
    run_test_on_fun(run2)
    run_test_on_fun(run)
    run_test_on_fun(run2)
    run_test_on_fun(run2)


def test_task_persist_local() -> None:
    val1 = random.randint(0, 5_000)
    val2 = random.randint(5_001, 10_000)

    did_run1 = False

    async def run1() -> None:
        nonlocal did_run1
        if did_run1:
            assert mod.local.get('value') == val1
        else:
            mod.local.set('value', val1)
        did_run1 = True

    did_run2 = False

    async def run2() -> None:
        nonlocal did_run2
        if did_run2:
            assert mod.local.get('value') == val2
            assert mod.local.get('value') != val1
        else:
            mod.local.set('value', val2)
        did_run2 = True

    run_test_on_fun(run1)
    assert did_run1
    run_test_on_fun(run1)

    run_test_on_fun(run2)
    assert did_run2
    run_test_on_fun(run2)
