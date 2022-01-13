"""
Module ``dagon.task``
#####################

Mid/high-level Task APIs

"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Iterable, Type, cast, overload

from dagon.util import T, dot_kebab_name

from .dag import TaskDAG, current_dag, result_of
from .task import Dependency, Task, TaskFunction

__all__ = [
    'define',
    'define_in',
    'define_fn_task',
    'gather',
    'task_from_function',
    'Task',
    'Dependency',
    'DependsArg',
    'TaskFunction',
    'result_of',
]


class InvalidParameters(RuntimeError):
    """
    Exception raised when a task function has an invalid parameter set.
    """


DependsArg = Iterable['Task[Any] | str']
"""
Type of an dependency list for a task. Either a :class:`Task`,
a ``str``, or a sequence thereof.
"""


def iter_deps(deps: DependsArg | None, order_only_depends: DependsArg | None) -> Iterable[Dependency]:
    for dep in (deps or ()):
        d = dep if isinstance(dep, str) else dep.name
        yield Dependency(d, is_order_only=False)

    for dep in (order_only_depends or ()):
        d = dep if isinstance(dep, str) else dep.name
        yield Dependency(d, is_order_only=True)


def task_from_function(fn: TaskFunction[T],
                       *,
                       default: bool = True,
                       name: str | None = None,
                       depends: DependsArg = (),
                       order_only_depends: DependsArg = (),
                       doc: str | None = None,
                       cls: Type[Task[T]] = Task,
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
    :param order_only_depends: A sequence of dependencies that are *order-only*,
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
        name = dot_kebab_name(fn.__name__)

    from inspect import isclass
    if not isclass(cls) or not issubclass(cls, Task):
        raise TypeError(f'`cls` argument must be a subclass of dagon.task.Task (Got {repr(cls)})')

    doc = (fn.__doc__ if doc is None else doc) or ''

    t: Task[T] = cls(
        name=name,
        fn=fn,
        depends=list(iter_deps(
            deps=depends or (),
            order_only_depends=order_only_depends or (),
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
    depends: DependsArg = (),
    order_only_depends: DependsArg = (),
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
                               order_only_depends=order_only_depends,
                               cls=cls,
                               disabled_reason=disabled_reason)
        return dag.add_task(t)

    return decorate_task_fn


def define(
    *,
    name: str | None = None,
    default: bool = False,
    depends: DependsArg = (),
    doc: str | None = None,
    order_only_depends: DependsArg = (),
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
        order_only_depends=order_only_depends,
        doc=doc,
        cls=cls,
        disabled_reason=disabled_reason,
    )


def define_in(
    dag: TaskDAG,
    *,
    name: str | None = None,
    default: bool = False,
    depends: DependsArg = (),
    order_only_dependss: DependsArg = (),
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
        order_only_depends=order_only_dependss,
        doc=doc,
        cls=cls,
        disabled_reason=disabled_reason,
    )


def define_fn_task(name: str,
                   fn: Callable[[], Awaitable[T]],
                   *,
                   dag: TaskDAG | None = None,
                   depends: DependsArg = (),
                   order_only_depends: DependsArg = (),
                   default: bool = False,
                   doc: str | None = None,
                   disabled_reason: str | None = None) -> Task[T]:
    """
    Create a task that executes a Python coroutine function.

    :param fn: The coroutine function to execute. See below.

    See :class:`.Task` for more parameter information.

    **The fn parameter**:

    The :obj:`fn` parameter must be given a coroutine function (defined with
    ``async def``) to execute. It *must* accept a :class:`.Context`
    as its first positional argument.

    The return value will be used as the result for the task.
    """
    async def _fn_trampoline() -> Any:
        return await fn()

    t = task_from_function(
        _fn_trampoline,
        name=name or dot_kebab_name(fn.__name__),
        default=default,
        depends=depends,
        order_only_depends=order_only_depends,
        doc=(fn.__doc__ or '') if doc is None else doc,
        disabled_reason=disabled_reason,
    )
    (dag or current_dag()).add_task(t)
    return t


@overload
def gather(name: str,
           tasks: DependsArg,
           *,
           dag: TaskDAG | None = None,
           default: bool = False,
           doc: str | None = None,
           order_only_depends: DependsArg = ()) -> Task[None]:
    ...


@overload
def gather(name: str,
           tasks: DependsArg,
           *,
           dag: TaskDAG | None = None,
           default: bool = False,
           doc: str | None = None,
           order_only_depends: DependsArg = (),
           return_value: T) -> Task[T]:
    ...


@overload
def gather(name: str,
           tasks: DependsArg,
           *,
           dag: TaskDAG | None = None,
           doc: str | None = None,
           order_only_depends: DependsArg = (),
           return_fn: Callable[[], T]) -> Task[T]:
    ...


def gather(name: str,
           tasks: DependsArg,
           *,
           dag: TaskDAG | None = None,
           default: bool = False,
           doc: str | None = None,
           order_only_depends: DependsArg = (),
           return_value: Any | None = None,
           return_fn: Callable[[], Any] | None = None) -> Task[Any]:
    """
    Declare a "gathering" task that simply depends on the given tasks but does
    nothing when it is executed.

    :param tasks: The tasks that the gather-task will depend on.
    :param return_value: Specify the result value for the gather-task, which
        will be returned from :func:`.Context.result_from` when used in
        downstream tasks.
    :param return_fn: Like :obj:`return_value`, but the function will be
        executed to generate the return value.

    .. warning::
        The result of ``return_fn()`` is returned *as-is*, with no ``await`` on
        the result. This means ``async`` functions for ``return_fn`` will
        probably not behave as expected!

    .. note::
        If neither :obj:`return_fn` nor :obj:`return_value` are specified, then
        the "result" of this task will be ``None``.

    See :func:`.define` for more parameter information.

    :return: The :class:`.Task` that was created
    """
    if return_value is not None:
        assert return_fn is None, 'Only specify one of `return_value` or `return_fn`, not both.'
        return_fn = lambda: return_value
    elif return_fn is None:
        return_fn = cast(Callable[[], T], lambda: None)
    else:
        assert callable(return_fn), f'Invalid `return_fn` object {repr(return_fn)} (must be callable)'

    # Required positional parameters
    async def _meta_fn() -> Any:
        assert return_fn
        return return_fn()

    return define_fn_task(
        name,
        _meta_fn,
        dag=dag,
        default=default,
        doc=doc,
        depends=tasks,
        order_only_depends=order_only_depends,
    )
