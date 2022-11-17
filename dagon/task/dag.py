from __future__ import annotations

import asyncio
import contextvars
import inspect
import itertools
import os
import signal
import traceback
import warnings
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from types import FrameType
from typing import (TYPE_CHECKING, Any, AsyncIterator, Awaitable, Callable, Iterable, Iterator, Mapping, NamedTuple,
                    Sequence, TypeVar, cast, overload)

from typing_extensions import Literal, TypeAlias

from dagon.core.result import Failure

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
        return TaskExecutor[Opaque](graph, exec_fn=cast(Any, exec_fn), catch_signals=True, fail_cancels=True)

    async def execute(
            self,
            marks: Iterable[str],
            *,
            exec_fn: Callable[[Task[T]], Awaitable[T]] = default_execute) -> Mapping[Task[Any], NodeResult[Task[Any]]]:
        ex = self.create_executor(marks, exec_fn=exec_fn)
        return await ex.run_all()


_CleanupFunc: TypeAlias = 'Callable[[], None|Awaitable[None]]'


class _ExecCtx(NamedTuple):
    exe: TaskExecutor[Any]
    task: Task[Any]
    add_graph_cleanup: Callable[[_CleanupFunc], None]
    add_cleanup_task: Callable[[asyncio.Task[Any]], None]
    cleanups: list[_CleanupFunc]


_TASK_CONTEXT = contextvars.ContextVar[_ExecCtx]('_TASK_CONTEXT')


def current_task() -> Task[Any]:
    return _TASK_CONTEXT.get().task


def cleanup(func: _CleanupFunc, *, when: Literal['now', 'graph-exit', 'task-exit'] = 'graph-exit') -> None:
    """
    Define a cleanup routine for the task or graph.

    :param func: A cleanup function to execute. If invoking this function
        returns an `~Awaitable` object, that object will be awaited before the
        task graph exits.
    :param when:
        Specify when the cleanup routine should begin executing. One of:

        - ``"graph-exit"`` - (Default) launch the cleanup routine only after no
          more tasks are running and the graph execution is shutting down.
        - ``"task-exit"`` - Launch the cleanup routine after the current task
          is finished executing.
        - ``"now"`` - Launch the cleanup routine immediately.

    Regardless of when a cleanup routine is launched, it will not block any
    process in graph execution except for graph shutdown, when all cleanup
    routines will be awaited.

    Use cleanup routines to execute actions that should be taken, but do not
    necessarily need to block the remainder of task execution. For example, the
    removal of temporary files should always occur, but it is not necessary that
    subsequent tasks need to wait for this to be complete.

    .. note::

        The cleanup function executes without a task context, so task-contextual
        actions must not be used!

    .. note::

        Exceptions that occur during the cleanup execution are ignored but do
        generate a `~RuntimeWarning`.

    .. note::

        Cleanup routines are executed asynchronously, but block graph execution
        shutdown. If a cleanup function takes too long to execute (Many
        seconds), it may be cancelled.
    """
    tctx = _TASK_CONTEXT.get()
    if when == 'task-exit':
        tctx.cleanups.append(func)
    elif when == 'graph-exit':
        tctx.add_graph_cleanup(func)
    elif when == 'now':
        t = asyncio.create_task(_run_cleanup(func), name=f'Cleanup {func!r} from task "{tctx.task.name}"')
        tctx.add_cleanup_task(t)
    else:
        _: int = when


async def _run_cleanup(cleaner: _CleanupFunc) -> None:
    r = cleaner()
    if inspect.isawaitable(r):
        await r


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
    ctx = _TASK_CONTEXT.get()
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


class _GraphContext(NamedTuple):
    cleanups: list[_CleanupFunc]
    cleanup_tasks: list[asyncio.Task[Any]]


_GRAPH_CONTEXT = contextvars.ContextVar[_GraphContext]('_GRAPH_CONTEXT')


class TaskExecutor(exec.SimpleExecutor[Task[T]]):
    """
    A DAG executor that executes task objects and allows the results of tasks
    to be shared between each other using the `.result_of` function.
    """
    def __init__(self, graph: LowLevelDAG[Task[T]], exec_fn: Callable[[Task[T]], Awaitable[T]], *, catch_signals: bool,
                 fail_cancels: bool):
        super().__init__(graph, exec_fn)
        self.__graph = graph
        self.__catch_signals = catch_signals
        self.__fail_cancels = fail_cancels

    @property
    def graph(self):
        """The low-level graph executed by this executor"""
        return self.__graph

    @asynccontextmanager
    async def running_context(self) -> AsyncIterator[None]:
        from dagon.event.cancel import CancellationToken
        ctx = _GraphContext([], [])
        async with AsyncExitStack() as st:
            await st.enter_async_context(super().running_context())
            st.enter_context(scope_set_contextvar(_GRAPH_CONTEXT, ctx))
            tok = CancellationToken.get_context_local()
            if tok is None:
                tok = CancellationToken()
                tok.set_context_local(tok)
                st.callback(lambda: tok.set_context_local(None))
            handle_signals: list[int] = []
            if self.__catch_signals:
                handle_signals.extend((signal.SIGINT, signal.SIGTERM))
                if os.name == 'nt':
                    handle_signals.append(signal.SIGBREAK)
                else:
                    handle_signals.append(signal.SIGQUIT)
            st.enter_context(self._handle_signals(handle_signals, tok))
            yield

        cleanup_tasks = set(ctx.cleanup_tasks)
        # Start all graph-exit cleanups
        for clean in ctx.cleanups:
            cleanup_tasks.add(asyncio.create_task(_run_cleanup(clean)))
        # Collect cleanup results
        await self._cleanup(cleanup_tasks)

    async def _cleanup(self, cleanups: set[asyncio.Task[Any]]) -> None:
        # Newly added
        all_tasks: set[asyncio.Task[Any]] = set()
        if cleanups:
            all_tasks |= cleanups
            _done, pending = await asyncio.wait(cleanups, timeout=3, return_when=asyncio.ALL_COMPLETED)
            if pending:
                print('Cleanup tasks are executing...')
                _done, pending = await asyncio.wait(pending, timeout=12, return_when=asyncio.ALL_COMPLETED)

        for t in all_tasks:
            self._print_cleanup_info(t)

    @staticmethod
    def _print_cleanup_info(task: asyncio.Task[Any]) -> None:
        if task.done():
            try:
                task.result()
            except:
                tb = traceback.format_exc()
                warnings.warn(f'Cleanup task {task!r} exited with an exception:\n{tb}', RuntimeWarning)
            return
        stack = task.get_stack()
        if not stack:
            tb = '<no traceback could be generated>'
        else:
            tb = traceback.format_stack(stack[-1])
        warnings.warn(f'Cleanup {task!r} did not complete in a reasonable time: {tb}', RuntimeWarning)

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
        from ..event import CancellationToken
        ctx = _ExecCtx(self,
                       task=node,
                       add_graph_cleanup=_GRAPH_CONTEXT.get().cleanups.append,
                       add_cleanup_task=_GRAPH_CONTEXT.get().cleanup_tasks.append,
                       cleanups=[])
        with scope_set_contextvar(_TASK_CONTEXT, ctx):
            result = await self.do_run_task(node)
            if isinstance(result.result, Failure) and self.__fail_cancels:
                cancel = CancellationToken.get_context_local()
                if cancel:
                    cancel.cancel()
            for clean in ctx.cleanups:
                ctx.add_cleanup_task(asyncio.create_task(_run_cleanup(clean)))
        return result

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
