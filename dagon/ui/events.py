from __future__ import annotations

from typing import NamedTuple
from dagon.core.result import NodeResult

from dagon.task.dag import OpaqueTask
from dagon.ui.message import Message
from dagon.ui.proc import ProcessResultUIInfo

from ..event import Event


class ProgressInfo(NamedTuple):
    progress: float | None


class UIEvents(NamedTuple):
    message: Event[Message] = Event()
    status: Event[str] = Event()
    task_result: Event[NodeResult[OpaqueTask]] = Event()
    progress: Event[ProgressInfo] = Event()
    process_done: Event[ProcessResultUIInfo] = Event()
