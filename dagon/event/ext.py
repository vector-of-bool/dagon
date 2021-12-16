from __future__ import annotations

import contextvars
from contextlib import ExitStack, asynccontextmanager, contextmanager
from typing import Any, AsyncIterator, Callable, Iterator
from dagon.event.cancel import CancellationToken

from dagon.ext.base import BaseExtension
from dagon.task.dag import OpaqueTask
from dagon.util import T, scope_set_contextvar

from .event import Event, EventMap

_CTX_EVENTS = contextvars.ContextVar[EventMap]('_CTX_EVENTS')


class _EventsContextLookup:
    @staticmethod
    def _ctx() -> EventMap:
        try:
            return _CTX_EVENTS.get()
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


class EventsExt(BaseExtension[None, None, None]):
    dagon_ext_name = 'dagon.events'

    @asynccontextmanager
    async def task_context(self, task: OpaqueTask) -> AsyncIterator[None]:
        map = EventMap()
        map.register('dagon.interval-start', Event[str]())
        map.register('dagon.interval-end', Event[None]())
        map.register('dagon.mark', Event[None]())
        with ExitStack() as st:
            st.enter_context(scope_set_contextvar(_CTX_EVENTS, map))
            st.enter_context(CancellationToken.scoped_context_local(CancellationToken()))
            yield


def interval_start(name: str) -> None:
    events['dagon.interval-start'].emit(name)


def interval_end() -> None:
    events['dagon.interval-end'].emit(None)


@contextmanager
def interval_context(name: str) -> Iterator[None]:
    interval_start(name)
    try:
        yield
    finally:
        interval_end()


def mark(name: str) -> None:
    events['dagon.mark'].emit(name)
