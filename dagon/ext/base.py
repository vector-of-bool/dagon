import argparse
from contextlib import nullcontext
from typing import (TYPE_CHECKING, AsyncContextManager, ClassVar, ContextManager, Generic, Sequence, cast)

from dagon.task.dag import OpaqueTask
from dagon.util import AsyncNullContext, typecheck

from .iface import AppDataT, GlobalDataT, IExtension, OpaqueTaskGraphView, TaskDataT
from .loader import ext_app_data, ext_global_data, ext_task_data


class BaseExtension(Generic[AppDataT, GlobalDataT, TaskDataT]):
    dagon_ext_name: str
    dagon_ext_requires: ClassVar[Sequence[str]] = ()
    dagon_ext_requires_opt: ClassVar[Sequence[str]] = ()

    def app_context(self) -> ContextManager[AppDataT]:
        return nullcontext(cast(AppDataT, None))

    def global_context(self, graph: OpaqueTaskGraphView) -> AsyncContextManager[GlobalDataT]:
        return AsyncNullContext(cast(GlobalDataT, None))

    def task_context(self, task: OpaqueTask) -> AsyncContextManager[TaskDataT]:
        return AsyncNullContext(cast(TaskDataT, None))

    def add_options(self, arg_parser: argparse.ArgumentParser) -> None:
        pass

    def handle_options(self, opts: argparse.Namespace) -> None:
        pass

    @classmethod
    def app_data(cls) -> AppDataT:
        return ext_app_data(cls.dagon_ext_name)

    @classmethod
    def global_data(cls) -> GlobalDataT:
        return ext_global_data(cls.dagon_ext_name)

    @classmethod
    def task_data(cls) -> TaskDataT:
        return ext_task_data(cls.dagon_ext_name)


if TYPE_CHECKING:
    typecheck(IExtension[int, str, bool])(BaseExtension[int, str, bool])
