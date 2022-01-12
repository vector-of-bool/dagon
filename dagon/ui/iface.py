from typing import AsyncContextManager
from typing_extensions import Protocol, runtime_checkable
from dagon.ext.iface import OpaqueTaskGraphView
from dagon.task.dag import OpaqueTask
from dagon.ui.events import UIEvents


@runtime_checkable
class I_UIExtension(Protocol):
    @property
    def dagon_ui_name(self) -> str:
        ...

    @property
    def dagon_ui_opt_name(self) -> str:
        ...

    def ui_global_context(self, graph: OpaqueTaskGraphView, events: UIEvents) -> AsyncContextManager[None]:
        ...

    def ui_task_context(self, task: OpaqueTask) -> AsyncContextManager[None]:
        ...
