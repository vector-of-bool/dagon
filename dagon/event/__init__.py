"""
Module ``dagon.event``
######################

Event handling

.. data:: events
    :type: dagon.event.EventMap

    A task-local `event map <dagon.event.EventMap>`.

    Comes pre-loaded with events:

    ``dagon.interval-start``
        `Event[str] <.Event>` : An event fired by `.interval_start`

    ``dagon.interval-end``
        `Event[None] <.Event>` : An event fired by `.interval_end`

    ``dagon.mark``
        `Event[str] <.Event>` : An event fired by `.mark`

    .. note:: Using this event map is only allowed following the initialization
        of the ``dagon.events`` extension, and only within the context of a
        task's execution. Each task receives its own fresh event map object.
"""

from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from typing import Any, AsyncIterator, Callable, Iterator

from ..ext.base import BaseExtension
from ..ext.iface import OpaqueTaskGraphView
from ..task.dag import OpaqueTask
from ..util import T
from .cancel import CancellationToken, CancelLevel, raise_if_cancelled
from .event import ConnectionToken, Event, EventMap

__all__ = [
    'events',
    'Event',
    'CancellationToken',
    'raise_if_cancelled',
    'ConnectionToken',
    'CancelLevel',
    'EventMap',
    'interval_context',
    'interval_end',
    'interval_start',
    'mark',
]


class _EventsExt(BaseExtension[None, EventMap, EventMap]):
    dagon_ext_name = 'dagon.events'

    @asynccontextmanager
    async def global_context(self, graph: OpaqueTaskGraphView) -> AsyncIterator[EventMap]:
        map = EventMap()
        with CancellationToken.ensure_context_local():
            yield map

    @asynccontextmanager
    async def task_context(self, task: OpaqueTask) -> AsyncIterator[EventMap]:
        map = self.global_data().clone()
        map.register('dagon.interval-start', Event[str]())
        map.register('dagon.interval-end', Event[None]())
        map.register('dagon.mark', Event[None]())
        yield map


class _EventsContextLookup:
    @staticmethod
    def _ctx() -> EventMap:
        try:
            return _EventsExt.task_data()
        except LookupError as e:
            raise RuntimeError('Cannot use dagon.event.events outside of a task context!') from e

    def __getitem__(self, key: str) -> Event[Any]:
        return self._ctx()[key]

    def get(self, key: str) -> Event[Any] | None:
        return self._ctx().get(key)

    def get_or_register(self, name: str, factory: Callable[[], Event[T]]) -> Event[T]:
        return self._ctx().get_or_register(name, factory)

    def register(self, name: str, ev: Event[T]) -> Event[T]:
        return self._ctx().register(name, ev)


events = _EventsContextLookup()


def interval_start(name: str) -> None:
    """
    Fire a ``dagon.interval-start`` event.

    .. note:: May only be called within a task-executing context.
    """
    events['dagon.interval-start'].emit(name)


def interval_end() -> None:
    """
    Fire a ``dagon.interval-end`` event.

    .. note:: May only be called within a task-executing context.
    """
    events['dagon.interval-end'].emit(None)


@contextmanager
def interval_context(name: str) -> Iterator[None]:
    """
    Create a scope for an `.interval_start` and `.interval_end` block with
    the given name.

    .. note:: May only be called within a task-executing context.
    """
    interval_start(name)
    try:
        yield
    finally:
        interval_end()


def mark(name: str) -> None:
    """
    Emit a ``dagon.mark`` event with the given name.

    .. note:: May only be called within a task-executing context.
    """
    events['dagon.mark'].emit(name)
