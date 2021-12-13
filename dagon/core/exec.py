"""
Low-level task graph execution APIs.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any, Awaitable, Callable, Generic, cast

from dagon.util import Opaque

from .result import (ExceptionInfo, TaskCancellation, TaskFailure, TaskResult, TaskSuccess)
from .task_graph import TaskGraph, TaskT


class TaskExceptionWarning(Warning):
    pass


TaskExecutorFunction = Callable[[TaskT], 'Awaitable[Any] | Any']
"""
Function type used to execute tasks. Must return a TaskExecResult or an awaitable thereof.

If the function returns ``TaskExecResult.Okay``, task execution will continue. If it
returns ``TaskExecResult.Stop``, task execution will stop enqueing tasks and wait for all
running tasks to finish.
"""


class SimpleExecutor(Generic[TaskT]):
    """
    A class that organizes and manages the execution of a task graph.

    This class creates asyncio.Task objects for tasks in the graph and enqueues
    them when ready.

    :param graph: The graph of tasks that will be executed.
    :param exec_fn: A function that is invoked to execute task objects
    :param start_predicate: A predicate function that tells the executor whether it should enqueue a given task.
        By default, this always returns True. This can be used to implement parallelism limits, for example.

    .. note::
        The :var:`graph` object will be modified by this executor's work!
    """
    def __init__(self,
                 graph: TaskGraph[TaskT],
                 exec_fn: TaskExecutorFunction[TaskT],
                 *,
                 loop: asyncio.AbstractEventLoop | None = None) -> None:
        loop = loop or asyncio.get_event_loop()
        #: Our task graph
        self._graph = graph
        #: The task-executing function
        self._exec_fn = cast(Callable[[TaskT], 'Opaque | Awaitable[Opaque]'], exec_fn)
        #: The set of asyncio.Task tasks that map to currently running tasks
        self._running: set[asyncio.Task[TaskResult[TaskT]]] = set()
        #: The result map for the tasks
        self._futures: dict[TaskT, 'asyncio.Future[TaskResult[TaskT]]'] = {
            t: asyncio.Future(loop=loop)
            for t in graph.all_nodes
        }
        self._loop = loop
        self._any_failed = False

    @property
    def has_pending_work(self):
        """Whether the executor has running or pending tasks remaining"""
        return self._graph.has_ready_nodes or bool(self._running)

    @property
    def has_running_work(self):
        """Whether there are any running tasks"""
        return bool(self._running)

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """The event loop associated with this executor"""
        return self._loop

    @property
    def any_failed(self) -> bool:
        """Whether any of the tasks in the graph have experienced failure/cancellation"""
        return self._any_failed

    def result_of(self, t: TaskT) -> Awaitable[TaskResult[TaskT]]:
        """Obtain the awaitable on the result of the given task"""
        return self._futures[t]

    def _start_more(self) -> None:
        """
        Enqueue all ready tasks in the associate task graph.
        """
        nodes_to_start = [n for n in self._graph.ready_nodes]
        new_tasks = set(self.loop.create_task(self._run_task_and_record(n)) for n in nodes_to_start)
        for n in nodes_to_start:
            self._graph.mark_running(n)
        self._running |= new_tasks

    async def run_some(self,
                       *,
                       interrupt: asyncio.Event | None = None,
                       timeout: float | None = None) -> set[TaskResult[TaskT]]:
        """
        Wait for any tasks to finish, or the 'interrupt' event is signaled.

        One must call :method:`.start_more` **before** this method.
        """
        if not self._any_failed:
            # There are no failures. Keep adding work
            self._start_more()

        if not self._running:
            # There is no work to run
            return set()

        # Create a future waiting on any of the tasks
        tasks_fut = asyncio.ensure_future(asyncio.wait(self._running, return_when=asyncio.FIRST_COMPLETED))

        # Start a future to wait on the interuption
        interrupt = interrupt or asyncio.Event()
        intr_fut = asyncio.ensure_future(interrupt.wait())

        # Wait on both the interrupt or on any of the tasks completing
        wait_both = asyncio.wait({tasks_fut, intr_fut}, return_when=asyncio.FIRST_COMPLETED, timeout=timeout)
        done = (await wait_both)[0]

        intr_fut.cancel()

        if tasks_fut not in done:
            # This occurs if we were signaled to return or if we hit our timeout
            return set()

        ret: set[TaskResult[TaskT]] = set()
        done, self._running = await tasks_fut
        for t in done:
            res = await t
            self._graph.mark_finished(res.task)
            ret.add(res)

        if not self._any_failed:
            # Check if any of the task executions represents a failure
            self._any_failed = any((not isinstance(r.result, TaskSuccess)) for r in ret)

        return ret

    async def run_all(self, *, interrupt: asyncio.Event | None = None) -> set[TaskResult[TaskT]]:
        """
        Execute all tasks in the task graph until completion, or until the 'interrupt' event is signaled.
        """
        ret: set[TaskResult[TaskT]] = set()
        while (not self._any_failed and self.has_pending_work) or self.has_running_work:
            ret |= await self.run_some(interrupt=interrupt)
        return ret

    async def _run_task_and_record(self, task_: TaskT) -> TaskResult[TaskT]:
        # Run the task in a way that captures all exceptions
        resdat = await self._run_task_noraise(task_)
        self._futures[task_].set_result(resdat)
        return resdat

    async def _run_task_noraise(self, task_: TaskT) -> TaskResult[TaskT]:
        try:
            retval = self._exec_fn(task_)
            # A successful execution with no errors. Capture the return value:
            if isinstance(retval, Awaitable):
                retval = await retval
            return TaskResult(task_, TaskSuccess(retval))
        except asyncio.CancelledError:
            # Cancellation
            return TaskResult(task_, TaskCancellation())
        except:  # pylint: disable=bare-except
            # An exception came from the task
            exc_info = sys.exc_info()
            assert exc_info[0]
            exc_info = cast(ExceptionInfo, exc_info)
            return TaskResult(task_, TaskFailure(exc_info))

    def run_all_until_complete(self) -> set[TaskResult[TaskT]]:
        return self.loop.run_until_complete(self.run_all())

    def run_some_until_complete(self) -> set[TaskResult[TaskT]]:
        return self.loop.run_until_complete(self.run_some())
