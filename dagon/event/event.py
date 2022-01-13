from __future__ import annotations

import traceback
import warnings
from typing import Any, Callable, Dict, Generic, Mapping, NamedTuple

from ..util import T, unused

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
    An object that can be used to subscribe-to or dispatch events.

    When an event is `emitted <.emit>` an associated event value will be passed.
    This same value will then be passed as the sole positional argument to all
    `handlers <.connect>`.
    """
    def __init__(self) -> None:
        self._handlers: HandlerMap[T] = {}
        self._tok_incr = 0

    def connect(self, handler: Handler[T]) -> ConnectionToken:
        """
        Register a new handler for this event. The handler must be callable with
        one positional argument, which will originate from the argument passed
        to `.emit`.

        :param handler: A function that handles events emitted by this object.

        :returns: A `.ConnectionToken` that can be used to unregister the
            event handler. See `.disconnect`.
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
        Emit an event, dispatching to all handlers.

        :param value: The value of the event. This object will be passed to all
            handlers.

        .. note:: Exceptions in any event handlers will be ignored with a
            warning.
        """
        # Make a copy of the handler list, in case an handler modifies it
        handlers = list(self._handlers.values())
        for h in handlers:
            try:
                h(value)
            except Exception:  # pylint: disable=broad-except
                tb = traceback.format_exc()
                warnings.warn(f'An event handler function raised an exception. It has been ignored. {tb}')


class EventMap:
    """
    A collection of named events.
    """
    def __init__(self, *, events: Mapping[str, Event[Any]] | None = None) -> None:
        self._events: dict[str, Event[Any]] = dict(events) if events is not None else {}

    def __getitem__(self, name: str) -> Event[Any]:
        """Obtain the event associated with `name`, or raise `KeyError`"""
        return self._events[name]

    def __contains__(self, name: str) -> bool:
        """Determine whether there is an event `name`"""
        return name in self._events

    def clone(self) -> EventMap:
        e = EventMap(events=self._events)
        return e

    def register(self, name: str, event: Event[T]) -> Event[T]:
        """
        Register a new `event` in the map with `name`. If `name` is already
        present in the map, raises `NameError`.
        """
        if name in self:
            raise NameError(f'Event name "{name}" is already registered')
        self._events[name] = event
        return event

    def get(self, key: str) -> Event[Any] | None:
        """Obtain the named event, or `None` if the event is not registered"""
        return self._events.get(key)

    def get_or_register(self, name: str, factory: Callable[[], Event[T]]) -> Event[T]:
        """Obtain the named event, or register a new event created by the given factory function."""
        e = self.get(name)
        if e is None:
            e = self.register(name, factory())
        return e
