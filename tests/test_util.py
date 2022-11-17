from __future__ import annotations

import functools
import pytest
from contextlib import ExitStack
from typing import Any, Awaitable, Callable, Iterable, Sequence

import dagon.task
from dagon.task.dag import TaskDAG, populate_dag_context
from dagon.tool import main
from dagon.util import T

NullaryFn = Callable[[], None]
Factory = Callable[..., T]


def dag_test(argv: Sequence[str] = ()) -> Callable[[Factory[Iterable[dagon.task.Task[Any]]]], NullaryFn]:
    def decorate(test_fn: Factory[Iterable[dagon.task.Task[Any]]]) -> NullaryFn:
        @functools.wraps(test_fn)
        def test_with_dag(**kwargs: Any):
            dag = TaskDAG('<test>')
            exts = main.get_extensions()
            with ExitStack() as st:
                st.enter_context(exts.app_context())
                st.enter_context(populate_dag_context(dag))
                runs = test_fn(**kwargs)
                nonlocal argv
                argv = ['--db-path=:memory:'] + list(argv)
                rc = main.run_for_dag(dag, exts, argv=argv, default_tasks=[t.name for t in runs])
                assert rc == 0

        return test_with_dag

    return decorate


def task_test(test_fn: Callable[[], Awaitable[Any]]) -> Callable[[], None]:
    @functools.wraps(test_fn)
    @pytest.mark.asyncio
    def run_test_task():
        dag = TaskDAG('<test>')
        exts = main.get_extensions()
        with ExitStack() as st:
            st.enter_context(exts.app_context())
            dagon.task.fn_task('test-task', test_fn, dag=dag, default=True)
            rc = main.run_for_dag(dag, exts, argv=[])
            assert rc == 0

    return run_test_task
