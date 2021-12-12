"""
Helpers for building a task Dag
"""

from __future__ import annotations

import contextvars
from contextlib import contextmanager
from typing import Any, Callable, Iterable, Iterator, Type

from dagon.util import T, kebab_name

from .dag import TaskDAG
from .task import Dependency, Task, TaskFunction


class InvalidParameters(RuntimeError):
    """
    Exception raised when a task function has an invalid parameter set.
    """


DepsArg = Iterable['Task[Any] | str']
"""
Type of an dependency list for a task. Either a :class:`Task`,
a ``str``, or a sequence thereof.
"""

_CURRENT_DAG = contextvars.ContextVar['TaskDAG | None']('_CURRENT_DAG', default=None)


def current_dag() -> TaskDAG:
    """
    Get the current thread/task-local Dag instance.
    """
    dag = _CURRENT_DAG.get()
    if dag is None:
        raise RuntimeError('There is no dag set for the current thread')
    return dag


def set_current_dag(dag: TaskDAG | None) -> None:
    """
    Set the current thread-local Dag instance.
    """
    _CURRENT_DAG.set(dag)


@contextmanager
def populate_dag_context(dag: TaskDAG) -> Iterator[None]:
    """
    Set the current thread-local Dag to ``dag``, and then restore the old
    thread-local Dag value when the context manager exists.

    :rtype: ContextManager[None]
    """
    tok = _CURRENT_DAG.set(dag)
    try:
        yield
    finally:
        _CURRENT_DAG.reset(tok)


def _iter_deps(deps: DepsArg | None, order_only_deps: DepsArg | None) -> Iterable[Dependency]:
    for dep in (deps or ()):
        d = dep if isinstance(dep, str) else dep.name
        yield Dependency(d, is_order_only=False)

    for dep in (order_only_deps or ()):
        d = dep if isinstance(dep, str) else dep.name
        yield Dependency(d, is_order_only=True)


def task_from_function(fn: TaskFunction[T],
                       *,
                       default: bool = True,
                       name: str | None = None,
                       depends: DepsArg = (),
                       order_only_deps: DepsArg = (),
                       doc: str | None = None,
                       cls: Type[Task[T]] = Task[T],
                       disabled_reason: str | None = None) -> Task[T]:
    """
    Create a :class:`Task` from the given function.

    :param fn: The function to create a task for.
    :param default: If ``True``, the task will be marked as "default"
        for execution.
    :param name: The name of the task. If not given, will use
        :func:`kebab_name` on the function's ``__name__`` to generate the name
        of the task.
    :param depends: Implicit dependencies of the task. Dependencies will
        attempt to be inferred from the signature of the given function using
        :func:`get_paramspec`, but additional dependencies can be listed using
        this parameter.
    :param order_only_deps: A sequence of dependencies that are *order-only*,
        meaning that they will not be transitively marked for execution as
        dependencies when generating the execution plan, but if they *are*
        otherwise marked, they will be required to complete successfully
        before this task is ready to be executed.
    :param doc: The help string for this task.
    :param cls: The class that should be instantiated for this task.
    :param disabled_reason: Disable the task with the given reason.

    For more parameter information, see :class:`Task`.
    """
    if name is None:
        name = kebab_name(fn.__name__)

    from inspect import isclass
    if not isclass(cls) or not issubclass(cls, Task):
        raise TypeError(f'`cls` argument must be a subclass of dagon.task.Task (Got {repr(cls)})')

    doc = (fn.__doc__ if doc is None else doc) or ''

    t: Task[T] = cls(
        name=name,
        fn=fn,
        deps=list(_iter_deps(
            deps=depends or (),
            order_only_deps=order_only_deps or (),
        )),
        default=default,
        doc=doc,
        disabled_reason=disabled_reason,
    )
    return t


def _task_decorator(
    dag: TaskDAG,
    *,
    default: bool,
    name: str | None,
    depends: DepsArg = (),
    order_only_deps: DepsArg = (),
    doc: str | None,
    cls: Type[Task[Any]],
    disabled_reason: str | None,
) -> Callable[[TaskFunction[T]], Task[T]]:
    def decorate_task_fn(fn: TaskFunction[T]) -> Task[T]:
        t = task_from_function(fn,
                               default=default,
                               name=name,
                               depends=depends,
                               doc=doc,
                               order_only_deps=order_only_deps,
                               cls=cls,
                               disabled_reason=disabled_reason)
        return dag.add_task(t)

    return decorate_task_fn


def define(
    *,
    name: str | None = None,
    default: bool = False,
    depends: DepsArg = (),
    doc: str | None = None,
    order_only_deps: DepsArg = (),
    cls: Type[Task[Any]] = Task,
    disabled_reason: str | None = None,
) -> Callable[[TaskFunction[T]], Task[T]]:
    """
    Add the given task function to the default DAG.

    .. seealso:: Refer to :func:`.task_from_function`
    """
    return _task_decorator(
        current_dag(),
        default=default,
        name=name,
        depends=depends,
        order_only_deps=order_only_deps,
        doc=doc,
        cls=cls,
        disabled_reason=disabled_reason,
    )


def define_in(
    dag: TaskDAG,
    *,
    name: str | None = None,
    default: bool = False,
    depends: DepsArg = (),
    order_only_deps: DepsArg = (),
    doc: str = '',
    cls: Type[Task[Any]] = Task,
    disabled_reason: str | None = None,
) -> Callable[[TaskFunction[T]], Task[T]]:
    """
    Create a task decorator that adds the given task to the given DAG
    """
    return _task_decorator(
        dag,
        default=default,
        name=name,
        depends=depends,
        order_only_deps=order_only_deps,
        doc=doc,
        cls=cls,
        disabled_reason=disabled_reason,
    )
