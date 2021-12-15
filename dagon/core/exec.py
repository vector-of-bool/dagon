"""
Low-level graph execution APIs.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any, Awaitable, Callable, Generic, cast

from dagon.util import Opaque, ReadyAwaitable

from .ll_dag import LowLevelDAG, NodeT
from .result import Cancellation, ExceptionInfo, Failure, NodeResult, Success

NodeExecutorFunction = Callable[[NodeT], 'Awaitable[Any] | Any']
"""
Function type used to "execute" a node.
"""


class SimpleExecutor(Generic[NodeT]):
    """
    A class that organizes and manages the execution of a :class:`LowLevelDAG`.

    This class creates :class:`asyncio.Task` objects for tasks in the graph and
    enqueues them when ready.

    :param graph: The graph of nodes that will be executed.
    :param exec_fn: A function that is invoked to execute node objects

    .. note::
        The :var:`graph` object will be modified by this executor's work!
    """
    def __init__(self,
                 graph: LowLevelDAG[NodeT],
                 exec_fn: NodeExecutorFunction[NodeT],
                 *,
                 loop: asyncio.AbstractEventLoop | None = None) -> None:
        loop = loop or asyncio.get_event_loop()
        #: Our graph
        self._graph = graph
        #: The node-executing function
        self._exec_fn = cast(Callable[[NodeT], 'Opaque | Awaitable[Opaque]'], exec_fn)
        #: The set of :class:`asyncio.Task` tasks that map to currently running nodes
        self._running: set[asyncio.Task[NodeResult[NodeT]]] = set()
        #: The result map for all nodes
        self._futures: dict[NodeT, 'asyncio.Future[NodeResult[NodeT]]'] = {
            t: asyncio.Future(loop=loop)
            for t in graph.all_nodes
        }
        self._loop = loop
        self._any_failed = False
        self._started = False
        self._finished = False

    @property
    def has_pending_work(self):
        """Whether the executor has running or pending work remaining"""
        return self._graph.has_ready_nodes or bool(self._running)

    @property
    def has_running_work(self):
        """Whether there is currently any running work"""
        return bool(self._running)

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """The event loop associated with this executor"""
        return self._loop

    @property
    def any_failed(self) -> bool:
        """Whether any of the nodes in the graph have experienced failure/cancellation"""
        return self._any_failed

    @property
    def finished(self):
        """Whether the execution is finished and no more work will be queued"""
        return self._finished

    def on_start(self) -> Awaitable[None]:
        return ReadyAwaitable(None)

    def on_finish(self) -> Awaitable[None]:
        return ReadyAwaitable(None)

    def result_of(self, t: NodeT) -> Awaitable[NodeResult[NodeT]]:
        """Obtain the awaitable on the result of the given node"""
        return self._futures[t]

    def _start_more(self) -> None:
        """
        Enqueue all ready tasks in the associate task graph.
        """
        nodes_to_start = [n for n in self._graph.ready_nodes]
        new_tasks = set(self.loop.create_task(self.do_run_node_outer(n)) for n in nodes_to_start)
        for n in nodes_to_start:
            self._graph.mark_running(n)
        self._running |= new_tasks

    async def run_some(self,
                       *,
                       interrupt: asyncio.Event | None = None,
                       timeout: float | None = None) -> set[NodeResult[NodeT]]:
        """
        Wait for any tasks to finish, or the 'interrupt' event is signaled.

        One must call :method:`.start_more` **before** this method.
        """
        assert not self.finished, 'run_some() called on finished executor'
        if not self._started and self.has_pending_work:
            await self.on_start()
            self._started = True

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

        ret: set[NodeResult[NodeT]] = set()
        done, self._running = await tasks_fut
        for t in done:
            res = await t
            self._graph.mark_finished(res.task)
            ret.add(res)

        if not self._any_failed:
            # Check if any of the executions represents a failure
            self._any_failed = any((not isinstance(r.result, Success)) for r in ret)

        if not self.has_pending_work or (self._any_failed and not self.has_running_work):
            self._finished = True
            await self.on_finish()

        return ret

    async def run_all(self, *, interrupt: asyncio.Event | None = None) -> set[NodeResult[NodeT]]:
        """
        Execute all nodes in the DAG until completion, or until the 'interrupt' event is signaled.
        """
        ret: set[NodeResult[NodeT]] = set()
        while (not self._any_failed and self.has_pending_work) or self.has_running_work:
            ret |= await self.run_some(interrupt=interrupt)
        return ret

    async def do_run_node_outer(self, node: NodeT) -> NodeResult[NodeT]:
        return await self._run_and_record(node)

    async def _run_and_record(self, node: NodeT) -> NodeResult[NodeT]:
        # Run the task and capture all results
        resdat = await self._run_noraise(node)
        self._futures[node].set_result(resdat)
        return resdat

    async def _run_noraise(self, node: NodeT) -> NodeResult[NodeT]:
        # Run the task in a way that captures all exceptions
        try:
            retval = await self.do_run_node_inner(node)
            # A successful execution with no errors. Capture the return value:
            return NodeResult(node, Success(retval))
        except asyncio.CancelledError:
            # Cancellation
            return NodeResult(node, Cancellation())
        except:  # pylint: disable=bare-except
            # An exception came from the task
            exc_info = sys.exc_info()
            assert exc_info[0]
            exc_info = cast(ExceptionInfo, exc_info)
            return NodeResult(node, Failure(exc_info))

    async def do_run_node_inner(self, node: NodeT) -> Any:
        ret = self._exec_fn(node)
        if isinstance(ret, Awaitable):
            ret = await ret
        return ret

    def run_all_until_complete(self) -> set[NodeResult[NodeT]]:
        return self.loop.run_until_complete(self.run_all())

    def run_some_until_complete(self) -> set[NodeResult[NodeT]]:
        return self.loop.run_until_complete(self.run_some())
