from __future__ import annotations

import contextvars
import difflib
import itertools
from contextlib import contextmanager
from typing import (Any, Awaitable, Callable, Iterable, Iterator, Mapping, TypeVar, cast, overload)

from dagon.core import exec, task_graph
from dagon.core.result import TaskResult, TaskSuccess
from dagon.util import NoneSuch, Opaque

from .task import InvalidTask, Task

T = TypeVar('T')

_OpaqueTask = Task[Opaque]


async def default_execute(task: Task[T]) -> T:
    return await task.function()


class TaskDAG:
    def __init__(self, name: str) -> None:
        self._name = name
        self._tasks: dict[str, _OpaqueTask] = {}

    @property
    def tasks(self) -> Iterable[Task[Any]]:
        """Tasks associated with this DAG"""
        return self._tasks.values()

    def add_task(self, task: Task[T]) -> Task[T]:
        if task.name in self._tasks:
            raise RuntimeError(f'More than one task with name "{task.name}"')
        for dep in task.depends:
            if dep.dep_name not in self._tasks.keys():
                # pylint: disable=cell-var-from-loop
                cand = max(self.tasks,
                           key=lambda t: difflib.SequenceMatcher(None, t.name, dep.dep_name).ratio(),
                           default=None)
                raise InvalidTask(NoneSuch[_OpaqueTask](dep.dep_name, cand))
        self._tasks[task.name] = cast(_OpaqueTask, task)
        return task

    def _collect(self, mark: str) -> Iterable[_OpaqueTask]:
        t = self._tasks[mark]
        yield t
        for d in t.depends:
            if d.is_order_only:
                continue
            yield from self._collect(d.dep_name)

    def create_executor(self,
                        marks: Iterable[str],
                        *,
                        exec_fn: Callable[[Task[T]], Awaitable[T]] = default_execute) -> exec.SimpleExecutor[Task[Any]]:
        by_name = {t.name: t for t in (itertools.chain.from_iterable(self._collect(m) for m in marks))}
        graph = task_graph.TaskGraph(
            nodes=by_name.values(),
            edges=itertools.chain.from_iterable(
                ((by_name[d.dep_name], t) for d in t.depends if d.dep_name in by_name) for t in by_name.values()),
        )
        return TaskExecutor(graph, exec_fn=exec_fn)

    async def execute(self,
                      marks: Iterable[str],
                      *,
                      exec_fn: Callable[[Task[T]], Awaitable[T]] = default_execute) -> set[TaskResult[Task[Any]]]:
        ex = self.create_executor(marks, exec_fn=exec_fn)
        return await ex.run_all()


_TaskResultMapType = Mapping[str, Awaitable[TaskResult[_OpaqueTask]]]
_CUR_TASK_RESULTS = contextvars.ContextVar[_TaskResultMapType]('_CUR_TASK_RESULTS')


def set_current_task_results(tres: _TaskResultMapType) -> None:
    _CUR_TASK_RESULTS.set(tres)


@overload
async def result_from(t: str) -> Any:
    ...


@overload
async def result_from(t: Task[T]) -> T:
    ...


async def result_from(t: str | Task[Any]) -> Any:
    if isinstance(t, Task):
        t = t.name
    res_fut = _CUR_TASK_RESULTS.get().get(t)
    if res_fut is None:
        raise InvalidTask(NoneSuch(t, None))
    res = (await res_fut).result
    assert isinstance(res, TaskSuccess), 'Cannot obtain the result from a task that was failed/cancelled'
    return res.value


class TaskExecutor(exec.SimpleExecutor[_OpaqueTask]):
    def __init__(self, graph: task_graph.TaskGraph[_OpaqueTask], exec_fn: Callable[[Task[T]], Awaitable[T]]):
        super().__init__(graph, self.__exec_task)
        self._by_name = {t.name: t for t in graph.all_nodes}
        self._exec = exec_fn

    async def __exec_task(self, task: _OpaqueTask) -> Opaque:
        resmap = self._create_partial_result_map(task)
        set_current_task_results(resmap)
        return await self._exec(task)

    def _create_partial_result_map(self, task: _OpaqueTask) -> _TaskResultMapType:
        """
        Create a map of the given task's dependencies to the result futures that
        correspond to those dependency tasks.
        """
        return {
            dep.dep_name: self.result_of(self._by_name[dep.dep_name])
            for dep in task.depends
            # order-only deps do not get a result entry
            if not dep.is_order_only
        }


_CURRENT_DAG = contextvars.ContextVar['TaskDAG | None']('_CURRENT_DAG', default=None)


def current_dag() -> TaskDAG:
    """
    Get the current thread/task-local Dag instance.
    """
    dag = _CURRENT_DAG.get()
    if dag is None:
        raise RuntimeError('There is no dag set for the current thread')
    return dag


def set_current_dag(dag: TaskDAG | None) -> None:
    """
    Set the current thread-local Dag instance.
    """
    _CURRENT_DAG.set(dag)


@contextmanager
def populate_dag_context(dag: TaskDAG) -> Iterator[None]:
    """
    Set the current thread-local Dag to ``dag``, and then restore the old
    thread-local Dag value when the context manager exists.

    :rtype: ContextManager[None]
    """
    tok = _CURRENT_DAG.set(dag)
    try:
        yield
    finally:
        _CURRENT_DAG.reset(tok)
