from __future__ import annotations

import functools
from contextlib import ExitStack
from typing import Callable, Sequence

from dagon.task.dag import TaskDAG, populate_dag_context
from dagon.tool import main

NullaryFn = Callable[[], None]


def dag_test(argv: Sequence[str] = (), tasks: Sequence[str] | None = None) -> Callable[[NullaryFn], NullaryFn]:
    def decorate(test_fn: NullaryFn) -> NullaryFn:
        @functools.wraps(test_fn)
        def test_with_dag():
            dag = TaskDAG('<test>')
            exts = main.get_extensions()
            with ExitStack() as st:
                st.enter_context(exts.app_context())
                st.enter_context(populate_dag_context(dag))
                test_fn()
                rc = main.run_for_dag(dag, exts, argv=argv, default_tasks=tasks)
                assert rc == 0

        return test_with_dag

    return decorate
