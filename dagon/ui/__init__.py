from __future__ import annotations
import os
import sys

import builtins
import argparse
import warnings
from contextlib import AsyncExitStack, ExitStack, asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Iterable, cast

from dagon.core.result import NodeResult
from dagon.ui.events import ProgressInfo, UIEvents
from dagon.ui.message import Message, MessageType

from ..ext.base import BaseExtension
from ..ext.iface import OpaqueTaskGraphView
from ..task.dag import OpaqueTask
from ..util import AsyncNullContext, Opaque, ReadyAwaitable, unused
from .iface import I_UIExtension
from .proc import ProcessResultUIInfo

if sys.version_info < (3, 10):
    from importlib_metadata import EntryPoint, entry_points
else:
    from importlib.metadata import EntryPoint, entry_points


class UILoadWarning(Warning):
    "Warning type for when warnings occur while loading a UI extension"


_EVENTS = UIEvents()


class _NullUI:
    dagon_ui_name = 'null'
    dagon_ui_opt_name = 'none'

    def _on_print(self, message: Message) -> None:
        builtins.print(message.content + '\n')

    @asynccontextmanager
    async def ui_global_context(self, graph: OpaqueTaskGraphView, events: UIEvents):
        with ExitStack() as st:
            st.enter_context(events.message.connect(self._on_print))
            yield

    def ui_task_context(self, task: OpaqueTask):
        return AsyncNullContext()


class _Ext(BaseExtension[None, I_UIExtension, None]):
    dagon_ext_name = 'dagon.ui'
    dagon_ext_requires = ['dagon.events']

    def __init__(self) -> None:
        eps = cast(Iterable[EntryPoint], entry_points(group='dagon.uis'))
        self._uis: dict[str, I_UIExtension] = {}
        self._chosen_ui: str | None = None
        for ep in eps:
            ui_inst = self._try_load_one(ep)
            if ui_inst:
                self._uis[ui_inst.dagon_ui_opt_name] = ui_inst

    @staticmethod
    def _try_load_one(ep: EntryPoint) -> None | I_UIExtension:
        ep_name: str = getattr(ep, 'name', '<unnamed>')
        try:
            cls: Opaque = ep.load()
        except BaseException as e:
            warnings.warn(f"An exception occurred while trying to load Dagon UI extension {ep!r}: {e}",
                          UILoadWarning,
                          source=e)
            return
        if not hasattr(cls, 'dagon_ui_name'):
            warnings.warn(f'UI extension object {cls!r} does not have a "dagon_ui_name" attribute', UILoadWarning)
            return
        ui_name = getattr(cls, 'dagon_ui_name')
        if not isinstance(ui_name, str):
            warnings.warn(f'UI extension object {cls!r} has a non-string "dagon_ui_name": {ui_name!r}', UILoadWarning)
            return
        if ui_name != ep_name:
            warnings.warn(
                f'UI Extension object {cls!r} has a mismatching UI name. Expected "{ep_name}", but got "{ui_name}"',
                UILoadWarning)
            return
        if not callable(cls):
            warnings.warn(f'UI extension entrypoint {cls!r} is not callable', UILoadWarning)
            return
        try:
            inst: Any = cls()
        except BaseException as e:
            warnings.warn(f'Instantiating/calling UI extension loader [{cls!r}] resulted in an exception',
                          UILoadWarning,
                          source=e)
            return
        if not isinstance(inst, I_UIExtension):
            warnings.warn(f'Generation UI extension object {inst!r} does not implement the necessary interface')
            return
        return inst

    def add_options(self, arg_parser: argparse.ArgumentParser) -> None:
        grp = arg_parser.add_argument_group('UI Options')
        grp.add_argument('--interface',
                         '-ui',
                         help='Set the user inferface kind',
                         choices=['auto', 'none'] + [u.dagon_ui_opt_name for u in self._uis.values()],
                         default='auto')
        return super().add_options(arg_parser)

    def handle_options(self, opts: argparse.Namespace) -> None:
        self._chosen_ui = opts.interface

    @asynccontextmanager
    async def global_context(self, graph: OpaqueTaskGraphView) -> AsyncIterator[I_UIExtension]:
        chosen = self._chosen_ui
        iface: I_UIExtension
        if chosen is None or chosen == 'none':
            iface = _NullUI()
        else:
            if chosen == 'auto':
                chosen = 'fancy' if os.isatty(sys.__stdout__.fileno()) else 'simple'
            iface = self._uis[chosen]
        async with AsyncExitStack() as st:
            await st.enter_async_context(iface.ui_global_context(graph, _EVENTS))
            yield iface

    @asynccontextmanager
    async def task_context(self, task: OpaqueTask):
        iface = self.global_data()
        async with AsyncExitStack() as st:
            await st.enter_async_context(iface.ui_task_context(task))
            yield

    def notify_result(self, result: NodeResult[OpaqueTask]) -> Awaitable[None]:
        _EVENTS.task_result.emit(result)
        return ReadyAwaitable(None)


def status(message: str) -> None:
    _EVENTS.status.emit(message)


def print(message: str, *, type: MessageType = MessageType.Print) -> None:
    _EVENTS.message.emit(Message(message, type))


def progress(value: float | None) -> None:
    _EVENTS.progress.emit(ProgressInfo(value))


def process_done(result: ProcessResultUIInfo) -> None:
    _EVENTS.process_done.emit(result)


unused(_Ext)
