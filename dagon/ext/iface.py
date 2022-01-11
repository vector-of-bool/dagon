import argparse
from typing import (AsyncContextManager, Awaitable, ContextManager, Generic, Iterable, TypeVar)

from dagon.core import ll_dag
from dagon.core.result import NodeResult
from dagon.task.dag import OpaqueTask
from typing_extensions import Protocol, runtime_checkable

AppDataT = TypeVar('AppDataT', covariant=True)
GlobalDataT = TypeVar('GlobalDataT', covariant=True)
TaskDataT = TypeVar('TaskDataT', covariant=True)

OpaqueTaskGraphView = ll_dag.DAGView[OpaqueTask]


@runtime_checkable
class IExtension(Generic[AppDataT, GlobalDataT, TaskDataT], Protocol):
    """
    The minimal interface required for any Dagon extension.
    """
    @property
    def dagon_ext_requires(self) -> Iterable[str]:
        """Extensions required by this extension"""
        ...

    @property
    def dagon_ext_requires_opt(self) -> Iterable[str]:
        """Extensions that are optionally used by this extension"""
        ...

    @property
    def dagon_ext_name(self) -> str:
        """The name of the extension"""
        ...

    def app_context(self) -> ContextManager[AppDataT]:
        """Synchronous application-level context setup for the extension."""
        ...

    def add_options(self, arg_parser: argparse.ArgumentParser) -> None:
        """Augment the command-line argument parser for this extension"""
        ...

    def handle_options(self, opts: argparse.Namespace) -> None:
        """Handle the commandd-line arguments for this extension"""
        ...

    def global_context(self, graph: OpaqueTaskGraphView) -> AsyncContextManager[GlobalDataT]:
        """The DAG-global context for this extension. Invoked during DAG execution startup/teardown."""
        ...

    def task_context(self, task: OpaqueTask) -> AsyncContextManager[TaskDataT]:
        """The task-local context for this extension. Invoked during task startup/teardown."""
        ...

    def notify_result(self, result: NodeResult[OpaqueTask]) -> Awaitable[None]:
        """Called by the DAG executor when a task completes, notifying the extension of the task's result."""
        ...
