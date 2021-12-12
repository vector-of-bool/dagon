from __future__ import annotations

import asyncio

import pytest

from . import dag as dag_mod
from .result import TaskResult, TaskSuccess
from .task import Dependency, Task


def test_simple() -> None:
    ran = 0

    async def simple_fn() -> None:
        nonlocal ran
        ran += 1
        print('Ran!')

    t1 = Task[None](name='foo', fn=simple_fn, deps=[], default=False)
    t2 = Task[None](name='bar', fn=simple_fn, deps=[Dependency('foo', is_order_only=False)], default=False)

    dag = dag_mod.TaskDAG('test')
    # Try in the wrong order
    with pytest.raises(RuntimeError, match='Unknown task name "foo"'):
        dag.add_task(t2)
    # Doesn't throw:
    dag.add_task(t1)
    # Now adding t2 will not throw
    dag.add_task(t2)

    # Run the things
    results = dag.execute(['bar'])
    results = asyncio.run(results)
    assert results == {
        TaskResult(t1, TaskSuccess(None)),
        TaskResult(t2, TaskSuccess(None)),
    }
    assert ran == 2


def test_simple_with_result():
    async def simple_two():
        return 2

    async def twice_prior() -> int:
        val: int | None = await dag_mod.result_from('foo')
        print(val)
        assert val == 2
        return 2 * val

    t1 = Task[int](name='foo', fn=simple_two, deps=[], default=False)
    t2 = Task[int](name='bar', fn=twice_prior, deps=[Dependency('foo', is_order_only=False)])
    dag = dag_mod.TaskDAG('test')
    dag.add_task(t1)
    dag.add_task(t2)
    results = asyncio.run(dag.execute(['bar']))
    assert results == {
        TaskResult(t1, TaskSuccess(2)),
        TaskResult(t2, TaskSuccess(4)),
    }
