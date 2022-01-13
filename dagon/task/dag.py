from __future__ import annotations

import asyncio
import contextvars
import itertools
import os
import signal
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from types import FrameType
from typing import (TYPE_CHECKING, Any, AsyncIterator, Awaitable, Callable, Iterable, Iterator, Mapping, NamedTuple,
                    Sequence, TypeVar, cast, overload)

from ..core import LowLevelDAG, NodeResult, Success, exec
from ..util import (NoneSuch, Opaque, first, nearest_matching, scope_set_contextvar)
from .task import DisabledTaskError, InvalidTask, Task

if TYPE_CHECKING:
    from dagon.event.cancel import CancellationToken

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
        return TaskExecutor[Opaque](graph, exec_fn=exec_fn, catch_signals=True)

    async def execute(
            self,
            marks: Iterable[str],
            *,
            exec_fn: Callable[[Task[T]], Awaitable[T]] = default_execute) -> Mapping[Task[Any], NodeResult[Task[Any]]]:
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
    ...


@overload
def result_of(task: str) -> Awaitable[Any]:
    ...


def result_of(task: Task[Any] | str) -> Awaitable[Any]:
    """
    Obtain the result of the given task.

    The other task must be a direct dependency of the currently executing task,
    and must not be an order-only dependency.
    """
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
    """
    A DAG executor that executes task objects and allows the results of tasks
    to be shared between each other using the `.result_of` function.
    """
    def __init__(self, graph: LowLevelDAG[Task[T]], exec_fn: Callable[[Task[T]], Awaitable[T]], catch_signals: bool):
        super().__init__(graph, exec_fn)
        self.__graph = graph
        self.__catch_signals = catch_signals

    @property
    def graph(self):
        """The low-level graph executed by this executor"""
        return self.__graph

    @asynccontextmanager
    async def running_context(self) -> AsyncIterator[None]:
        from dagon.event.cancel import CancellationToken
        async with AsyncExitStack() as st:
            await st.enter_async_context(super().running_context())
            tok = CancellationToken.get_context_local()
            if tok is None:
                tok = CancellationToken()
                tok.set_context_local(tok)
                st.callback(lambda: tok.set_context_local(None))
            handle_signals: list[int] = []
            if self.__catch_signals:
                handle_signals.extend((signal.SIGINT, signal.SIGTERM, signal.SIGQUIT))
                if os.name == 'nt':
                    handle_signals.append(signal.SIGBREAK)
            st.enter_context(self._handle_signals(handle_signals, tok))
            yield

    @contextmanager
    def _handle_signals(self, sigs: Sequence[int], cancel: CancellationToken) -> Iterator[None]:
        loop = asyncio.get_event_loop()
        prev_handlers = [signal.signal(n, lambda n, fr: self._do_stop_threadsafe(n, fr, cancel, loop)) for n in sigs]
        try:
            yield
        finally:
            for sig, prev in zip(sigs, prev_handlers):
                signal.signal(sig, prev)

    @staticmethod
    def _do_stop_threadsafe(_signum: int, _frame: None | FrameType, cancel: CancellationToken,
                            loop: asyncio.AbstractEventLoop) -> None:
        loop.call_soon_threadsafe(cancel.cancel)

    async def do_run_node(self, node: Task[T]) -> NodeResult[Task[T]]:
        """
        Overridden from `SimpleExecutor.do_run_node`. Sets up the context to
        allow result sharing between tasks. Calls `.do_run_task` to execute
        the task within the scope that allows for result sharing.
        """
        with scope_set_contextvar(_CTX_EXEC, _ExecCtx(self, node)):
            return await self.do_run_task(node)

    async def do_run_task(self, node: Task[T]) -> NodeResult[Task[T]]:
        return await super().do_run_node(node)


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
