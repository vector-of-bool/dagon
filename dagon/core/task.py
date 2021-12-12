from __future__ import annotations

from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Generic, Iterable, NamedTuple, Union)

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
                 deps: Iterable[Dependency],
                 default: bool = True,
                 doc: str = '',
                 disabled_reason: str | None = None):
        self._name = name
        self._doc = doc
        self._fn = fn
        self._deps = tuple(deps)
        self._default_enabeld = default
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
    def depends(self) -> Iterable[Dependency]:
        """The dependencies (by name) of this task"""
        return iter(self._deps)

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
