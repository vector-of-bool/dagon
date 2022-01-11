from __future__ import annotations

from contextlib import AsyncExitStack

from dagon.core import ll_dag
from dagon.core.result import NodeResult
from dagon.task.dag import OpaqueTask, TaskExecutor
from dagon.util import Opaque

from .loader import ExtLoader


class ExtAwareExecutor(TaskExecutor[Opaque]):
    """
    An extension-aware task executor. Executes a task graph and loads the appropriate
    extension contexts during execution
    """
    def __init__(self, exts: ExtLoader, graph: ll_dag.LowLevelDAG[OpaqueTask]) -> None:
        super().__init__(graph, self.__exec_task)
        self._exts = exts
        self._exts_st = AsyncExitStack()

    async def __exec_task(self, task: OpaqueTask) -> Opaque:
        return await task.function()

    async def on_start(self) -> None:
        await self._exts_st.enter_async_context(self._exts.global_context(ll_dag.DAGView(self.graph)))

    async def on_finish(self) -> None:
        await self._exts_st.aclose()

    async def do_run_task(self, node: OpaqueTask) -> NodeResult[OpaqueTask]:
        async with self._exts.task_context(node):
            result = await super().do_run_task(node)
            await self._exts.notify_result(result)
            return result
