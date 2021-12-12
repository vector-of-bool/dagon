from __future__ import annotations

import contextvars
import difflib
import itertools
from typing import Any, Awaitable, Iterable, Mapping, TypeVar, cast, overload

from dagon.util import NoneSuch, Opaque

from . import exec, task_graph
from .result import TaskResult, TaskSuccess
from .task import InvalidTask, Task

T = TypeVar('T')

_OpaqueTask = Task[Opaque]


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

    async def execute(self, marks: Iterable[str]) -> set[TaskResult[_OpaqueTask]]:
        tasks = set(itertools.chain.from_iterable(self._collect(m) for m in marks))
        ex = _DagExecutor(tasks)
        return await ex.execute()


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


class _DagExecutor:
    def __init__(self, tasks: Iterable[_OpaqueTask]):
        self._tasks = {t.name: t for t in tasks}
        self._graph = task_graph.TaskGraph(
            nodes=self._tasks.values(),
            edges=itertools.chain.from_iterable(((self._tasks[d.dep_name], t) for d in t.depends
                                                 if d.dep_name in self._tasks) for t in self._tasks.values()),
        )
        self._executor = exec.SimpleExecutor(self._graph, self._run_task)

    async def execute(self) -> set[TaskResult[_OpaqueTask]]:
        return await self._executor.run_all()

    async def _run_task(self, task: _OpaqueTask) -> Opaque:
        resmap = self._create_partial_result_map(task)
        set_current_task_results(resmap)
        return await task.function()

    def _create_partial_result_map(self, task: _OpaqueTask) -> _TaskResultMapType:
        """
        Create a map of the given task's dependencies to the result futures that
        correspond to those dependency tasks.
        """
        return {
            dep.dep_name: self._executor.result_of(self._tasks[dep.dep_name])
            for dep in task.depends
            # order-only deps do not get a result entry
            if not dep.is_order_only
        }
