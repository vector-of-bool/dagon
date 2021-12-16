from __future__ import annotations

from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Generic, Iterable, NamedTuple, Sequence, Union)

from dagon.util import NoneSuch, T

IDependency = Union['Task[Any]', str]
TaskFunction = Callable[[], Awaitable[T]]


class Dependency(NamedTuple):
    """
    A dependency upon a task, which may be order-only
    """
    #: The name of the depended-on Task
    dep_name: str
    #: Whether the dependency is order-only
    is_order_only: bool


class Task(Generic[T]):
    """
    A task that wraps a callable function and contains a set of dependencies.

    :param name: The name of the task.
    :param fn: The function to execute for this task.
    :param deps: The dependencies of this task
    :param default: Whether this task should be considered "default" in task graphs of which it is a member.
    :param doc: A documentation string for this task.
    """
    def __init__(self,
                 name: str,
                 fn: TaskFunction[T],
                 depends: Iterable[Dependency],
                 default: bool = True,
                 doc: str = '',
                 disabled_reason: str | None = None):
        self._name = name
        self._doc = doc
        self._fn = fn
        self._deps = tuple(depends)
        self._is_default = default
        self._disabled_reason = disabled_reason

    @property
    def name(self) -> str:
        """The name of the task"""
        return self._name

    @property
    def doc(self) -> str:
        """The docstring associated with the task"""
        return self._doc

    @property
    def function(self) -> TaskFunction[T]:
        """The function that is associated with this task"""
        return self._fn

    @property
    def depends(self) -> Sequence[Dependency]:
        """The dependencies (by name) of this task"""
        return self._deps

    @property
    def is_disabled(self):
        """Whether this task is disabled"""
        return self.disabled_reason is not None

    @property
    def disabled_reason(self) -> str | None:
        """The explanation of why this task is disabled (or None if it is not disabled)"""
        return self._disabled_reason

    @property
    def is_default(self):
        """Whether this task is marked as a default task"""
        return self._is_default

    if not TYPE_CHECKING:

        def __call__(self, *args: Any, **kwds: Any) -> Any:
            raise RuntimeError(
                'Do not call tasks as functions! Add them as dependencies and use dagon.task.result_from()')

    def __repr__(self) -> str:
        return f'<dagon.core.Task "{self.name}">'


class InvalidTask(RuntimeError):
    """
    Exception raised when an invalid task is requested to execute.
    """
    def __init__(self, n: NoneSuch[Task[Any]]) -> None:
        super().__init__(f'Unknown task name "{n.key}"')
        self.key = n.key
        self.candidate = n.candidate


class DisabledTaskError(RuntimeError):
    """Exception thrown when an attempt is made to execute a disabled task"""
    def __init__(self, stack: Sequence[str], reason: str) -> None:
        self._task_stack = list(stack)
        root_task = stack[0]
        disabled = stack[-1]
        reqs_str = ''.join(f', which requires "{t}"' for t in stack[1:])

        super().__init__(f'Cannot mark "{root_task}"{reqs_str}, because "{disabled}" is disabled: {reason}')

    @property
    def task_stack(self) -> Sequence[str]:
        """Get the stack of tasks that led to the failure.
        The final task in the stack is the disabled task, while the first
        is the initial requested task."""
        return self._task_stack
