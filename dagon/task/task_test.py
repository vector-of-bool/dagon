import itertools
from contextlib import ExitStack
from typing import Any, Callable, Coroutine

import dagon.task as mod
from dagon.ext.exec import ExtAwareExecutor
from dagon.tool import main
from dagon.util.testing import async_test

from ..core.result import Failure, NodeResult, Success
from .dag import TaskDAG, result_of

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
        await result_of(string)

    @mod.define_in(dag, depends=[string])
    async def print_string2() -> None:
        await result_of(string)
        await result_of(string)

    @mod.define_in(dag, depends=[print_string, print_string2])
    async def final_tgt() -> None:
        pass

    results = await dag.execute(['print-string'])
    assert set(results.values()) == {
        NodeResult(string, Success('hello')),
        NodeResult(print_string, Success(None)),
    }

    results = await dag.execute(['print-string2'])
    assert set(results.values()) == {
        NodeResult(string, Success('hello')),
        NodeResult(print_string2, Success(None)),
    }

    results = await dag.execute(['final-tgt'])
    assert set(results.values()) == {
        NodeResult(string, Success('hello')),
        NodeResult(print_string, Success(None)),
        NodeResult(print_string2, Success(None)),
        NodeResult(final_tgt, Success(None)),
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
        assert set(result.values()) == {
            NodeResult(_first, Success(None)),
            NodeResult(_second, Success(None)),
        }
    else:
        assert set(result.values()) == {NodeResult(_second, Success(None))}


@async_test
async def test_order_only() -> None:
    for _, use_oo_deps in itertools.product(range(100), (True, False)):
        # Run this repeatedly to try and force the ordering to fail
        await _run_oo_test(use_oo_deps)


def run_test_on_fun(fn: Callable[[], Coroutine[None, None, None]], **kw: Any) -> None:
    with ExitStack() as st:
        exts = main.get_extensions()
        st.enter_context(exts.app_context())
        t = mod.task_from_function(fn, **kw)
        dag = TaskDAG('Test')
        dag.add_task(t)
        graph = dag.low_level_graph([t.name])
        results = ExtAwareExecutor(exts, graph, catch_signals=False).run_all_until_complete()
        for f in results.values():
            if isinstance(f.result, Failure):
                f.result.reraise()
            assert isinstance(f.result, Success)
