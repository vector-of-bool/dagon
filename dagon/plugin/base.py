from contextlib import nullcontext
from typing import ContextManager, Generic, cast
from dagon.plugin.iface import GlobalDataT, OpaqueTaskGraphView, TaskDataT
from dagon.task.dag import OpaqueTask


class BasePlugin(Generic[GlobalDataT, TaskDataT]):
    dagon_plugin_requires = []

    def global_context(self, graph: OpaqueTaskGraphView) -> ContextManager[GlobalDataT]:
        return cast(ContextManager[GlobalDataT], nullcontext())

    def task_context(self, task: OpaqueTask) -> ContextManager[TaskDataT]:
        return cast(ContextManager[TaskDataT], nullcontext())
