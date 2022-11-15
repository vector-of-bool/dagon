import random

import pytest

from .. import db as db_mod
from .. import persist as mod
from ..task.task_test import run_test_on_fun


@pytest.fixture
def db() -> db_mod.Database:
    return db_mod.Database.open(':memory:')


EGGS = mod.Key[int]('eggs')


@pytest.mark.asyncio
async def test_persist_global(db: db_mod.Database) -> None:
    with mod.set_global_persistor(mod.in_database(db)):
        assert not await mod.globl.has(EGGS)
        await mod.globl.set(EGGS, 22)
        assert await mod.globl.get(EGGS) == 22

    with mod.set_global_persistor(mod.in_database(db)):
        # Reopen the same persistor
        assert await mod.globl.has(EGGS)
        assert await mod.globl.get(EGGS) == 22

        # Modify it again
        await mod.globl.set(EGGS, 42)
        assert await mod.globl.get(EGGS) == 42

    with mod.set_global_persistor(mod.in_database(db)):
        assert await mod.globl.get(EGGS) == 42


@pytest.mark.asyncio
async def test_persist_local(db: db_mod.Database) -> None:
    tid1 = db.get_task_rowid('example1')
    tid2 = db.get_task_rowid('example2')

    with mod.set_task_local_persistor(mod.in_database(db, task=tid1)):
        assert not await mod.local.has(EGGS)
        await mod.local.set(EGGS, 12)
        assert await mod.local.get(EGGS) == 12

    with mod.set_task_local_persistor(mod.in_database(db, task=tid2)):
        assert not await mod.local.has(EGGS)
        await mod.local.set(EGGS, 72)
        assert await mod.local.get(EGGS) == 72

    with mod.set_task_local_persistor(mod.in_database(db, task=tid1)):
        # Restored from the prior task ID
        assert await mod.local.has(EGGS)
        assert await mod.local.get(EGGS) == 12


def test_task_persist_global() -> None:
    val = random.randint(0, 99999)

    tv = mod.Key[int]('test_value')

    async def run() -> None:
        await mod.globl.set(tv, val)

    async def run2() -> None:
        assert await mod.globl.get(tv) == val

    run_test_on_fun(run)
    run_test_on_fun(run2)
    run_test_on_fun(run)
    run_test_on_fun(run2)
    run_test_on_fun(run2)


def test_task_persist_local() -> None:
    val1 = random.randint(0, 5_000)
    val2 = random.randint(5_001, 10_000)

    did_run1 = False
    value = mod.Key[int]('value')

    async def run1() -> None:
        nonlocal did_run1
        if did_run1:
            assert await mod.local.get(value) == val1
        else:
            await mod.local.set(value, val1)
        did_run1 = True

    did_run2 = False

    async def run2() -> None:
        nonlocal did_run2
        if did_run2:
            assert await mod.local.get(value) == val2
            assert await mod.local.get(value) != val1
        else:
            await mod.local.set(value, val2)
        did_run2 = True

    run_test_on_fun(run1)
    assert did_run1
    run_test_on_fun(run1)

    run_test_on_fun(run2)
    assert did_run2
    run_test_on_fun(run2)
