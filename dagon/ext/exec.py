from __future__ import annotations

from contextlib import AsyncExitStack
from typing import Any

from dagon.core import ll_dag
from dagon.task.dag import TaskExecutor
from dagon.task.task import Task

from .loader import ExtLoader


class ExtAwareExecutor(TaskExecutor[Any]):
    def __init__(self, exts: ExtLoader, graph: ll_dag.LowLevelDAG[Task[Any]]) -> None:
        super().__init__(graph, self.__exec_task)
        self._exts = exts
        self._exts_st = AsyncExitStack()

    async def __exec_task(self, task: Task[Any]) -> Any:
        return await task.function()

    async def on_start(self) -> None:
        await self._exts_st.enter_async_context(self._exts.global_context(ll_dag.DAGView(self.graph)))

    async def on_finish(self) -> None:
        await self._exts_st.aclose()

    async def do_run_task_outer(self, node: Task[Any]) -> Any:
        async with self._exts.task_context(node):
            return await super().do_run_task(node)
