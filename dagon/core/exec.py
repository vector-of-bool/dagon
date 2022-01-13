"""
Module ``dagon.core.exec``
##########################

Low-level asynchronous graph execution APIs.
"""

from __future__ import annotations

import asyncio
import sys
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any, AsyncContextManager, Awaitable, Callable, Generic, Mapping, cast

from ..util import AsyncNullContext, Opaque, ensure_awaitable, unused
from .ll_dag import LowLevelDAG, NodeT
from .result import Cancellation, ExceptionInfo, Failure, NodeResult, Success

NodeExecutorFunction = Callable[[NodeT], 'Awaitable[Any] | Any']
"""
A type alias of any function type used to "execute" a node. This is simply any
callable that accepts a node object and returns a value or an awaitable.
Whether the returned value is awaitable affects the behavior of the
`.SimpleExecutor` class handling that result.
"""


class SimpleExecutor(Generic[NodeT]):
    """
    A class that organizes and manages the execution of a `~.LowLevelDAG`.

    This class creates :class:`asyncio.Task` objects for tasks in the graph and
    enqueues them when ready.

    :param graph: The graph of nodes that will be executed.
    :param exec_fn: A function that is invoked to "execute" node objects.
    :param loop: The event loop that will be used during execution. If omitted,
        will use the current thread's thread loop. There *must* be a valid
        thread loop for the executor to initialize with, as it needs to prime
        the result `asyncio.Future` objects.

    .. note::
        The :obj:`graph` object will be modified by this executor's work! If you
        want to retain the original graph unmodified, use `.LowLevelDAG.copy`
        to create a copy of the graph to give to the executor.

    This class can be derived from to customize an "hook" certain events. Refer
    to the `.running_scope`, and `.do_run_node` methods, which are intended to
    be overridden for the purpose of intercepting certain graph events.

    .. note::
        It is safe to add additional nodes and edges to the graph while it is
        being executed, except for additional inbound edges to finished/running
        nodes, which is forbidden by `.LowLevelDAG.add` and guarded with an
        exception.
    """
    def __init__(self,
                 graph: LowLevelDAG[NodeT],
                 exec_fn: NodeExecutorFunction[NodeT],
                 *,
                 loop: asyncio.AbstractEventLoop | None = None) -> None:
        loop = loop or asyncio.get_event_loop()
        self.__graph = graph
        'Our graph'
        self.__exec_fn = cast(Callable[[NodeT], 'Opaque | Awaitable[Opaque]'], exec_fn)
        'The node-executing function'
        self.__running: set[asyncio.Task[NodeResult[NodeT]]] = set()
        'The set of :class:`asyncio.Task` tasks that map to currently running nodes'
        self.__futures: dict[NodeT, 'asyncio.Future[NodeResult[NodeT]]'] = {
            t: asyncio.Future(loop=loop)
            for t in graph.all_nodes
        }
        'The result map for all nodes'
        self.__loop = loop
        'The event loop for this executor'
        self.__any_failed = False
        'Whether any nodes have failed'
        self.__started = False
        'Whether we have called on_finish'
        self.__running_scope = AsyncExitStack()
        'A context manager in scope while running the graph'
        self.__finished = False
        'Whether we have closed the running scope'

    @property
    def has_pending_work(self) -> bool:
        """
        Whether the executor has running or pending work remaining.

        .. note::
            It is possible that `.finished` is `True` while `.has_pending_work`
            is *also* `True`: If any node executions raise an exception, then
            the executor will stop enqueueing work and will declare itself to
            be finished once all running work has finished.
        """
        return self.__graph.has_ready_nodes or bool(self.__running)

    @property
    def has_running_work(self) -> bool:
        """Whether there is currently any running work"""
        return bool(self.__running)

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """The event loop associated with this executor"""
        return self.__loop

    @property
    def any_failed(self) -> bool:
        """
        Whether any of the nodes in the graph have experienced failure/cancellation.
        """
        return self.__any_failed

    @property
    def finished(self) -> bool:
        """
        Whether the execution is finished and no more work will be queued.

        This value becomes `True` immediately after the final `NodeResult` is
        collected by `.run_some` and no more nodes are set to be executed.

        .. note::
            It is possible that `.has_pending_work` is `True` while `.finished`
            is *also* `True`: If any node executions raise an exception, then
            the executor will stop enqueueing work and will declare itself to
            be finished once all running work has finished.
        """
        return self.__finished

    def result_of(self, node: NodeT) -> Awaitable[NodeResult[NodeT]]:
        """Obtain the awaitable on the result of the given node"""
        return self.__futures[node]

    def _start_more(self) -> None:
        """
        Enqueue all ready tasks in the associated task graph.
        """
        nodes_to_start = list(self.__graph.ready_nodes)
        new_tasks = set(self.loop.create_task(self.do_run_node(n)) for n in nodes_to_start)
        for n in nodes_to_start:
            self.__graph.mark_running(n)
        self.__running |= new_tasks

    async def run_some(self,
                       *,
                       interrupt: asyncio.Event | None = None,
                       timeout: float | None = None) -> Mapping[NodeT, NodeResult[NodeT]]:
        """
        Start pending nodes, and wait for one or more running nodes to finish,
        or until the `interrupt` `~asyncio.Event` is signaled, or `timeout` is
        reached.

        :param interrupt: An `~asyncio.Event` that can be used to interrupt the
            executor waiting on any nodes to finish.
        :param timeout: A timeout (in seconds) after which the call to
            `.run_some` will stop waiting on pending results and return.

        .. note:: Asserts that `.finished` is `False`

        This method will return a `set` of `.NodeResult` objects that represent
        the nodes that completed during this execution of `.run_some`. The
        returned `set` will never be empty unless `interrupt` is triggered or
        `timeout` is reached.

        .. note:: The `interrupt` event will not be cleared after we receive
            an interuption. If called in a loop with the same `~asyncio.Event`
            object, be sure to `~asyncio.Event.clear` the event before each call
            to `.run_some`.
        """
        assert not self.finished, 'run_some() called on finished executor'
        if not self.__started and self.has_pending_work:
            await self.__running_scope.enter_async_context(self.running_context())
            self.__started = True

        if not self.__any_failed:
            # There are no failures. Keep adding work
            self._start_more()

        if not self.__running:
            # There is no work to run
            return {}

        # Create a future waiting on any of the tasks
        tasks_fut = asyncio.ensure_future(asyncio.wait(self.__running, return_when=asyncio.FIRST_COMPLETED))

        # Start a future to wait on the interuption
        interrupt = interrupt or asyncio.Event()
        intr_fut = asyncio.ensure_future(interrupt.wait())

        # Wait on both the interrupt or on any of the tasks completing
        wait_both = asyncio.wait({tasks_fut, intr_fut}, return_when=asyncio.FIRST_COMPLETED, timeout=timeout)
        done = (await wait_both)[0]

        intr_fut.cancel()

        if tasks_fut not in done:
            # This occurs if we were signaled to return or if we hit our timeout
            return {}

        ret: dict[NodeT, NodeResult[NodeT]] = {}
        done, self.__running = await tasks_fut
        for t in done:
            res = await t
            self.__graph.mark_finished(res.task)
            ret[res.task] = res

        if not self.any_failed:
            # Check if any of the executions represents a failure
            self.__any_failed = any((not isinstance(r.result, Success)) for r in ret.values())

        if not self.has_pending_work or (self.any_failed and not self.has_running_work):
            self.__finished = True
            await self.__running_scope.aclose()

        return ret

    async def run_all(self, *, interrupt: asyncio.Event | None = None) -> Mapping[NodeT, NodeResult[NodeT]]:
        """
        Execute all nodes in the DAG until `.finished` is `True`, or until the
        `interrupt` `~asyncio.Event` is signaled.

        Returns a `set` of all `NodeResult` results.
        """
        ret: dict[NodeT, NodeResult[NodeT]] = {}
        while (not self.__any_failed and self.has_pending_work) or self.has_running_work:
            ret.update(await self.run_some(interrupt=interrupt))
        return ret

    def run_all_until_complete(self) -> Mapping[NodeT, NodeResult[NodeT]]:
        """
        Like `.run_all` but calls :func:`asyncio.run` with the coroutine.
        """
        return self.loop.run_until_complete(self.run_all())

    def run_some_until_complete(self) -> Mapping[NodeT, NodeResult[NodeT]]:
        """
        Like `.run_some` but calls :func:`asyncio.run` with the coroutine.
        """
        return self.loop.run_until_complete(self.run_some())

    def running_context(self) -> AsyncContextManager[None]:
        """
        A context manager that is in scope while the graph is running, and
        exited when the graph finishes execution.

        This method is a no-op by default. It is intended to be overridden by a
        derived class.
        """
        if TYPE_CHECKING:
            unused(self)
        return AsyncNullContext()

    def do_run_node(self, node: NodeT) -> Awaitable[NodeResult[NodeT]]:
        """
        Customization layer for handling node execution. Derived classes may
        override this method to intercept nodes before and after they are
        executed.

        .. note::
            Derived classes *should* invoke `SimpleExecutor.do_run_node()` as
            part of their derived implementation, as this method performs
            important book-keeping for tasks.

        .. note::
            This method should never raise an exception, if at all possible. If
            the execution of a node raises an exception, that failure will be
            captured at a lower layer and enclosed in a `.Failure` inside of
            the `.NodeResult` returned by `.do_run_node`.
        """
        return self._run_and_record(node)

    async def _run_and_record(self, node: NodeT) -> NodeResult[NodeT]:
        "Run the task and capture all results"
        resdat = await self._run_noraise(node)
        self.__futures[node].set_result(resdat)
        return resdat

    async def _run_noraise(self, node: NodeT) -> NodeResult[NodeT]:
        "Run the task in a way that captures all exceptions"
        try:
            retval = await ensure_awaitable(self.__exec_fn(node))
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
