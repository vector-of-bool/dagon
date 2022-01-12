from __future__ import annotations

from contextlib import AsyncExitStack, asynccontextmanager
from typing import AsyncIterator

from ..core import ll_dag
from ..core.result import NodeResult
from ..task.dag import OpaqueTask, TaskExecutor
from ..util import Opaque
from .loader import ExtLoader


class ExtAwareExecutor(TaskExecutor[Opaque]):
    """
    An extension-aware task executor. Executes a task graph and loads the appropriate
    extension contexts during execution
    """
    def __init__(self, exts: ExtLoader, graph: ll_dag.LowLevelDAG[OpaqueTask], catch_signals: bool) -> None:
        super().__init__(graph, self.__exec_task, catch_signals)
        self._exts = exts

    async def __exec_task(self, task: OpaqueTask) -> Opaque:
        return await task.function()

    @asynccontextmanager
    async def running_context(self) -> AsyncIterator[None]:
        async with AsyncExitStack() as st:
            await st.enter_async_context(self._exts.global_context(ll_dag.DAGView(self.graph)))
            await st.enter_async_context(super().running_context())
            yield

    async def do_run_task(self, node: OpaqueTask) -> NodeResult[OpaqueTask]:
        async with self._exts.task_context(node):
            result = await super().do_run_task(node)
            await self._exts.notify_result(result)
            return result
