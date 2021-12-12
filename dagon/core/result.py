from __future__ import annotations

import types
from dataclasses import dataclass
from typing import Any, Generic, NamedTuple, NoReturn, Tuple, Type

from .task_graph import TaskT

ExceptionInfo = Tuple[Type[BaseException], BaseException, types.TracebackType]


class TaskSuccess(NamedTuple):
    """
    Represents a successful execution of a task to completion
    """
    value: Any
    "The value that resulted from the task's execution (i.e. the return value)"


class TaskCancellation:
    """
    Represents that a task was cancelled before completing.
    """


class TaskFailure(NamedTuple):
    """
    Represents that a task failed with an exception
    """
    exc_info: ExceptionInfo
    "The exception information triple that was raised while executing the task"

    @property
    def exception_type(self) -> None | Type[BaseException]:
        """The exception type associated with this failure (if applicable)"""
        return self.exc_info[0]

    @property
    def exception(self) -> None | BaseException:
        """The exception associated with this failure (if applicable)"""
        return self.exc_info[1]

    @property
    def traceback(self) -> None | types.TracebackType:
        """The exception traceback associated with this failure (if applicable)"""
        return self.exc_info[2]

    def reraise(self) -> NoReturn:
        """
        Re-raise the exception from this failure, if one is present, otherwise
        raises :class:`RuntimeError`.

        This function does not return.
        """
        exc = self.exception
        if exc is None:
            raise RuntimeError('No exception associated with this failure')
        raise exc


@dataclass(frozen=True)
class TaskResult(Generic[TaskT]):
    """
    The result of a task's execution
    """
    task: TaskT
    "The task that produced the result"
    result: TaskSuccess | TaskCancellation | TaskFailure
    "The actual result: A success, cancellation, or failure"
