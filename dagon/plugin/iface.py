from typing import ContextManager, Generic, Iterable, TypeVar
from typing_extensions import Protocol
from dagon.core import ll_dag
from dagon.task.dag import OpaqueTask

GlobalDataT = TypeVar('GlobalDataT', covariant=True)
TaskDataT = TypeVar('TaskDataT', covariant=True)

OpaqueTaskGraphView = ll_dag.DAGView[OpaqueTask]


class IPlugin(Generic[GlobalDataT, TaskDataT], Protocol):
    @property
    def dagon_plugin_requires(self) -> Iterable[str]:
        """Plugins required by this plugin"""
        ...

    @property
    def dagon_plugin_name(self) -> str:
        """The name of the plugin"""
        ...

    def global_context(self, graph: OpaqueTaskGraphView) -> ContextManager[GlobalDataT]:
        ...

    def task_context(self, task: OpaqueTask) -> ContextManager[TaskDataT]:
        ...
