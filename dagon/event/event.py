from __future__ import annotations

import contextvars
import traceback
import warnings
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generic, Iterator, NamedTuple

from dagon.plugin.base import BasePlugin
from dagon.task.dag import OpaqueTask
from dagon.util import T, scope_set_contextvar, unused

Handler = Callable[[T], Any]
HandlerMap = Dict['ConnectionToken', Handler[T]]


class ConnectionToken(NamedTuple):
    key: int
    event: Event[Any]

    def __enter__(self) -> None:
        pass

    def __exit__(self, _unused: Any, _unused2: Any, _unused3: Any) -> None:
        unused(_unused, _unused2, _unused3)
        self.event.disconnect(self)


class Event(Generic[T]):
    """
    An object that can be used to subscribe-to/dispatch events.
    """
    def __init__(self) -> None:
        self._handlers: HandlerMap[T] = {}
        self._tok_incr = 0

    def connect(self, handler: Handler[T]) -> ConnectionToken:
        """
        Register a new handler for this event.

        :param handler: A function that handles events emitted by this object.

        :returns: A :class:`ConnectionToken` that can be used to unregister the
            event handler. See :func:`disconnect`.
        """
        token = ConnectionToken(self._tok_incr, self)
        self._tok_incr += 1
        self._handlers[token] = handler
        return token

    def disconnect(self, token: ConnectionToken) -> None:
        """
        Disconnect a previously connected event handler.

        :param token: A token obtained from :func:`connect`.
        """
        self._handlers.pop(token)

    def emit(self, value: T) -> None:
        """
        Emit an event, dispatching to all handlers
        """
        # Make a copy of the handler list, in case an handler modifies it
        handlers = list(self._handlers.values())
        for h in handlers:
            try:
                h(value)
            except Exception:  # pylint: disable=broad-except
                traceback.print_exc()
                warnings.warn('An event handler function raised an exception. It has been ignored.')


class EventMap:
    def __init__(self) -> None:
        self._events: dict[str, Event[Any]] = {}

    def __getitem__(self, name: str) -> Event[Any]:
        return self._events[name]

    def __contains__(self, key: str) -> bool:
        return key in self._events

    def register(self, name: str, event: Event[T]) -> Event[T]:
        if name in self:
            raise NameError(f'Event name "{name}" is already registered')
        self._events[name] = event
        return event

    def get(self, key: str) -> Event[Any] | None:
        return self._events.get(key)

    def get_or_register(self, name: str, factory: Callable[[], Event[T]]) -> Event[T]:
        e = self.get(name)
        if e is None:
            e = self.register(name, factory())
        return e


_CTX_EVENTS = contextvars.ContextVar[EventMap]('_CTX_EVENTS')


class _EventsContextLookup:
    @staticmethod
    def _ctx() -> EventMap:
        try:
            return _CTX_EVENTS.get()
        except LookupError:
            raise RuntimeError('Cannot use dagon.event.events outside of a task context!')

    def __getitem__(self, key: str) -> Event[Any]:
        return self._ctx()[key]

    def get(self, key: str) -> Event[Any] | None:
        return self._ctx().get(key)

    def get_or_register(self, name: str, factory: Callable[[], Event[T]]) -> Event[T]:
        return self._ctx().get_or_register(name, factory)

    def register(self, name: str, ev: Event[T]) -> Event[T]:
        return self._ctx().register(name, ev)


events = _EventsContextLookup()


class EventsPlugin(BasePlugin[None, None]):
    dagon_plugin_name = 'dagon.events'

    def task_context(self, task: OpaqueTask):
        map = EventMap()
        map.register('dagon.interval-start', Event[str]())
        map.register('dagon.interval-end', Event[None]())
        map.register('dagon.mark', Event[None]())
        return scope_set_contextvar(_CTX_EVENTS, map)


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
