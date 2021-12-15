from __future__ import annotations

import contextvars
import itertools
from contextlib import contextmanager
from typing import (Any, Awaitable, Callable, Iterable, Iterator, NamedTuple, TypeVar, cast, overload)

from dagon.core import exec
from dagon.core.ll_dag import LowLevelDAG
from dagon.core.result import NodeResult, Success
from dagon.util import NoneSuch, Opaque, first, nearest_matching

from .task import DisabledTaskError, InvalidTask, Task

T = TypeVar('T')

OpaqueTask = Task[Opaque]


class NoDependencyError(RuntimeError):
    def __init__(self, from_task: Task[Any], to_task: Task[Any]):
        super().__init__(f'Task "{from_task.name}" does not have a direct dependency on "{to_task.name}"')
        self.from_task = from_task
        self.to_task = to_task


class IsOrderOnlyDependencyError(RuntimeError):
    def __init__(self, from_task: Task[Any], to_task: Task[Any]) -> None:
        super().__init__(f'The dependcy of "{from_task.name}" on "{to_task.name}" is an order-only dependency')
        self.from_task = from_task
        self.to_task = to_task


async def default_execute(task: Task[T]) -> T:
    return await task.function()


class TaskDAG:
    def __init__(self, name: str) -> None:
        self._name = name
        self._tasks: dict[str, OpaqueTask] = {}

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
                cand = nearest_matching(dep.dep_name, self.tasks, key=lambda t: t.name)
                raise InvalidTask(NoneSuch[OpaqueTask](dep.dep_name, cand))
        self._tasks[task.name] = cast(OpaqueTask, task)
        return task

    def _collect(self, mark: str, stack: list[str], marked: set[str]) -> Iterable[OpaqueTask]:
        if mark in marked:
            return

        # Check that we actually have this task
        if mark not in self._tasks:
            cand = nearest_matching(mark, self.tasks, key=lambda t: t.name)
            raise InvalidTask(NoneSuch[OpaqueTask](mark, cand))

        # Append to the search stack
        stack.append(mark)
        t = self._tasks[mark]

        # Check if it was disabled:
        if t.is_disabled:
            assert t.disabled_reason
            raise DisabledTaskError(stack, t.disabled_reason)

        # Yield this item
        yield t
        marked.add(t.name)

        # Yield each hard dependency
        for d in t.depends:
            if d.is_order_only or d.dep_name in marked:
                continue
            yield from self._collect(d.dep_name, stack, marked)

        # Pop from the search stack
        stack.pop()

    def low_level_graph(self, marks: Iterable[str]) -> LowLevelDAG[Task[Any]]:
        by_name = {t.name: t for t in (itertools.chain.from_iterable(self._collect(m, [], set()) for m in marks))}
        return LowLevelDAG[Task[Any]](
            nodes=by_name.values(),
            edges=itertools.chain.from_iterable(
                ((by_name[d.dep_name], t) for d in t.depends if d.dep_name in by_name) for t in by_name.values()),
        )

    def create_executor(self,
                        marks: Iterable[str],
                        *,
                        exec_fn: Callable[[Task[T]], Awaitable[T]] = default_execute) -> exec.SimpleExecutor[Task[Any]]:
        graph = self.low_level_graph(marks)
        return TaskExecutor[Opaque](graph, exec_fn=exec_fn)

    async def execute(self,
                      marks: Iterable[str],
                      *,
                      exec_fn: Callable[[Task[T]], Awaitable[T]] = default_execute) -> set[NodeResult[Task[Any]]]:
        ex = self.create_executor(marks, exec_fn=exec_fn)
        return await ex.run_all()


class _ExecCtx(NamedTuple):
    exe: TaskExecutor[Any]
    task: Task[Any]


_CTX_EXEC = contextvars.ContextVar[_ExecCtx]('_CTX_EXECUTING_GRAPH')


def current_task() -> Task[Any]:
    return _CTX_EXEC.get().task


@overload
def result_of(task: Task[T]) -> Awaitable[T]:
    """
    Obtain the result of the given task.

    The other task must be a direct dependency of the currently executing task,
    and must not be an order-only dependency.
    """
    ...


@overload
def result_of(task: str) -> Awaitable[Any]:
    ...


def result_of(task: Task[Any] | str) -> Awaitable[Any]:
    ctx = _CTX_EXEC.get()
    ctx_task = ctx.task
    if isinstance(task, str):
        find = (n for n in ctx.exe.graph.all_nodes if n.name == task)
        try:
            task, = find
        except ValueError:
            raise NameError(f'No known task named "{task}"')
    find = (d for d in ctx_task.depends if d.dep_name == task.name)
    try:
        dep = first(find)
    except ValueError:
        raise NoDependencyError(ctx_task, task)
    if dep.is_order_only:
        raise IsOrderOnlyDependencyError(ctx_task, task)
    ares = ctx.exe.result_of(task)
    return _await_result(ares, task.name)


async def _await_result(ares: Awaitable[NodeResult[Any]], name: str) -> Any:
    res = await ares
    if isinstance(res.result, Success):
        return res.result.value
    raise RuntimeError(f'Attempted to obtain result of task "{name}", which is failed/cancelled')


class TaskExecutor(exec.SimpleExecutor[Task[T]]):
    def __init__(self, graph: LowLevelDAG[Task[T]], exec_fn: Callable[[Task[T]], Awaitable[T]]):
        super().__init__(graph, exec_fn)
        self.__graph = graph

    @property
    def graph(self):
        """The low-level graph executed by this executor"""
        return self.__graph

    async def do_run_node_outer(self, node: Task[T]) -> NodeResult[Task[T]]:
        tok = _CTX_EXEC.set(_ExecCtx(self, node))
        try:
            result = await self.do_run_task_outer(node)
        finally:
            _CTX_EXEC.reset(tok)
        return result

    async def do_run_task_outer(self, node: Task[T]) -> NodeResult[Task[T]]:
        return await super().do_run_node_outer(node)


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
