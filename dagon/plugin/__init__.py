from __future__ import annotations

import contextvars
from contextlib import ExitStack
from typing import Any, NamedTuple, overload

from dagon.core import ll_dag
from dagon.task.dag import TaskExecutor
from dagon.task.task import Task
from dagon.util import Opaque

from .iface import GlobalDataT, IPlugin, TaskDataT


class _GlobalPluginContext(NamedTuple):
    plugins: dict[str, Opaque]
    stack: ExitStack


class _TaskPluginContext(NamedTuple):
    plugins: dict[str, Opaque]
    stack: ExitStack


# The per-context plugin instances
_GLOBAL_PLUGINS_BY_CTX = contextvars.ContextVar[_GlobalPluginContext]('_GLOBAL_PLUGINS_BY_CTX')
_TASK_PLUGINS_BY_CTX = contextvars.ContextVar[_TaskPluginContext]('_TASK_PLUGINS_BY_CTX')


@overload
def global_plugin_data(plugin: IPlugin[GlobalDataT, Any]) -> GlobalDataT:
    ...


@overload
def global_plugin_data(plugin: str) -> Any:
    ...


def global_plugin_data(plugin: str | IPlugin[Any, Any]) -> Any:
    if not isinstance(plugin, str):
        plugin = plugin.dagon_plugin_name
    ctx = _GLOBAL_PLUGINS_BY_CTX.get()
    try:
        return ctx.plugins[plugin]
    except KeyError:
        raise RuntimeError(f'No plugin "{plugin}" is loaded in the global context')


@overload
def task_plugin_data(plugin: IPlugin[Any, TaskDataT]) -> TaskDataT:
    ...


@overload
def task_plugin_data(plugin: str) -> Any:
    ...


def task_plugin_data(plugin: str | IPlugin[Any, Any]) -> Any:
    if not isinstance(plugin, str):
        plugin = plugin.dagon_plugin_name
    ctx = _TASK_PLUGINS_BY_CTX.get()
    try:
        return ctx.plugins[plugin]
    except KeyError:
        raise LookupError(f'No plugin "{plugin}" is loaded in the task-local context')


class PluginSet:
    def __init__(self) -> None:
        self._loaded: dict[str, IPlugin[Opaque, Opaque]] = {}
        # Build the event map

    def load(self, plugin: IPlugin[Any, Any]) -> None:
        if plugin.dagon_plugin_name in self._loaded:
            raise NameError(f'Plugin "{plugin.dagon_plugin_name}" is already loaded')
        self._loaded[plugin.dagon_plugin_name] = plugin

    def _init_plugins(self, ctx: _GlobalPluginContext, name: str, graph: ll_dag.DAGView[Task[Opaque]]) -> None:
        if name in ctx.plugins:
            return
        reg = self._loaded[name]
        for req in reg.dagon_plugin_requires:
            if req not in self._loaded:
                raise RuntimeError(f'No plugin "{req}" is registered, but is required by "{name}"')
            self._init_plugins(ctx, req, graph)
        ctx.plugins[name] = ctx.stack.enter_context(reg.global_context(graph))

    def on_dag_start(self, graph: ll_dag.DAGView[Task[Opaque]]) -> None:
        ctx = _GlobalPluginContext({}, ExitStack())
        _GLOBAL_PLUGINS_BY_CTX.set(ctx)
        for pname in self._loaded:
            self._init_plugins(ctx, pname, graph)

    def on_dag_end(self) -> None:
        ps = _GLOBAL_PLUGINS_BY_CTX.get()
        ps.stack.close()

    def on_task_start(self, task: Task[Opaque]) -> None:
        ctx = _GLOBAL_PLUGINS_BY_CTX.get()
        task_ctx = _TaskPluginContext({}, ExitStack())
        _TASK_PLUGINS_BY_CTX.set(task_ctx)
        # Iterating over the dict entries will match the order that there were initialized globally:
        for name in ctx.plugins.keys():
            task_ctx.plugins[name] = task_ctx.stack.enter_context(self._loaded[name].task_context(task))

    def on_task_end(self) -> None:
        task_ctx = _TASK_PLUGINS_BY_CTX.get()
        task_ctx.stack.close()


class PluginAwareExecutor(TaskExecutor[Any]):
    def __init__(self, plugins: PluginSet, graph: ll_dag.LowLevelDAG[Task[Any]]) -> None:
        super().__init__(graph, self.__exec_task)
        self._plugins = plugins

    async def __exec_task(self, task: Task[Any]) -> Any:
        return await task.function()

    def on_start(self) -> None:
        self._plugins.on_dag_start(ll_dag.DAGView(self.graph))

    def on_finish(self) -> None:
        self._plugins.on_dag_end()

    async def do_run_task_outer(self, node: Task[Any]) -> Any:
        self._plugins.on_task_start(node)
        try:
            return await super().do_run_task_outer(node)
        finally:
            self._plugins.on_task_end()
