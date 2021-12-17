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

from .event import Event, ConnectionToken, EventMap
from .ext import events, interval_context, interval_end, interval_start, mark
from .cancel import CancellationToken, CancelLevel, raise_if_cancelled

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
