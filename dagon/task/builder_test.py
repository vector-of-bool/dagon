import itertools

import dagon.task as mod
from dagon.util.testing import async_test

from .dag import TaskDAG, result_from
from ..core.result import TaskResult, TaskSuccess

# pyright: reportUnusedFunction=false


def test_add() -> None:
    dag = TaskDAG('test')

    @mod.define_in(dag)
    async def meow() -> None:
        pass

    @mod.define_in(dag, depends=[meow])
    async def inner() -> int:
        return 0


@async_test
async def test_add_and_run() -> None:
    dag = TaskDAG('test')

    @mod.define_in(dag)
    async def string() -> str:
        return 'hello'

    @mod.define_in(dag, depends=[string])
    async def print_string() -> None:
        await result_from(string)

    @mod.define_in(dag, depends=[string])
    async def print_string2() -> None:
        await result_from(string)
        await result_from(string)

    @mod.define_in(dag, depends=[print_string, print_string2])
    async def final_tgt() -> None:
        pass

    results = await dag.execute(['print-string'])
    assert results == {
        TaskResult(string, TaskSuccess('hello')),
        TaskResult(print_string, TaskSuccess(None)),
    }

    results = await dag.execute(['print-string2'])
    assert results == {
        TaskResult(string, TaskSuccess('hello')),
        TaskResult(print_string2, TaskSuccess(None)),
    }

    results = await dag.execute(['final-tgt'])
    assert results == {
        TaskResult(string, TaskSuccess('hello')),
        TaskResult(print_string, TaskSuccess(None)),
        TaskResult(print_string2, TaskSuccess(None)),
        TaskResult(final_tgt, TaskSuccess(None)),
    }


async def _run_oo_test(use_oo_deps: bool) -> None:
    value = 0

    dag = TaskDAG('test')

    @mod.define_in(dag)
    async def _first() -> None:
        nonlocal value
        value = 1

    @mod.define_in(dag, order_only_dependss=[_first])
    async def _second() -> None:
        nonlocal value
        if use_oo_deps:
            assert value == 1, 'Value should have been set'
        else:
            assert value == 0, 'Value should not have been set'

    if use_oo_deps:
        result = await dag.execute([_first.name, _second.name])
    else:
        result = await dag.execute([_second.name])

    if use_oo_deps:
        assert result == {
            TaskResult(_first, TaskSuccess(None)),
            TaskResult(_second, TaskSuccess(None)),
        }
    else:
        assert result == {TaskResult(_second, TaskSuccess(None))}


@async_test
async def test_order_only() -> None:
    for _, use_oo_deps in itertools.product(range(100), (True, False)):
        # Run this repeatedly to try and force the ordering to fail
        await _run_oo_test(use_oo_deps)


# def run_test_on_fun(fn: Callable[[], Coroutine[None, None, None]], **kw: Any) -> None:
#     t = mod.task_from_function(fn, **kw)
#     dag = TaskDAG('Test')
#     dag.add_task(t)
#     plan = dag.mark([t.name])
#     fails = dag.run_until_complete(plan, ExecutionOptions())
#     for f in fails:
#         assert f.exception is not None
#         raise f.exception
