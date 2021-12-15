import argparse
from typing import (AsyncContextManager, ContextManager, Generic, Iterable, TypeVar)

from dagon.core import ll_dag
from dagon.task.dag import OpaqueTask
from typing_extensions import Protocol, runtime_checkable

AppDataT = TypeVar('AppDataT', covariant=True)
GlobalDataT = TypeVar('GlobalDataT', covariant=True)
TaskDataT = TypeVar('TaskDataT', covariant=True)

OpaqueTaskGraphView = ll_dag.DAGView[OpaqueTask]


@runtime_checkable
class IExtension(Generic[AppDataT, GlobalDataT, TaskDataT], Protocol):
    @property
    def dagon_ext_requires(self) -> Iterable[str]:
        """Extensions required by this extension"""
        ...

    @property
    def dagon_ext_name(self) -> str:
        """The name of the extension"""
        ...

    def app_context(self) -> ContextManager[AppDataT]:
        ...

    def add_options(self, arg_parser: argparse.ArgumentParser) -> None:
        ...

    def handle_options(self, opts: argparse.Namespace) -> None:
        ...

    def global_context(self, graph: OpaqueTaskGraphView) -> AsyncContextManager[GlobalDataT]:
        ...

    def task_context(self, task: OpaqueTask) -> AsyncContextManager[TaskDataT]:
        ...
