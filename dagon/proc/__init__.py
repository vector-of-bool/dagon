from __future__ import annotations
from typing import Iterable

from dagon.task.task import Dependency

from ..fs import Pathish
from ..task import DependsArg, Task, TaskDAG, current_dag
from .proc import CommandLine, LineHandler, LogLine, OutputMode, ProcessOutputResult, UpdateStatus, run as run_proc


def _iter_deps(deps: DependsArg | None, order_only_depends: DependsArg | None) -> Iterable[Dependency]:
    for dep in (deps or ()):
        d = dep if isinstance(dep, str) else dep.name
        yield Dependency(d, is_order_only=False)

    for dep in (order_only_depends or ()):
        d = dep if isinstance(dep, str) else dep.name
        yield Dependency(d, is_order_only=True)


def define_cmd_task(name: str,
                    cmd: CommandLine,
                    *,
                    dag: TaskDAG | None = None,
                    cwd: Pathish | None = None,
                    doc: str = '',
                    output: OutputMode = 'accumulate',
                    check: bool = True,
                    default: bool = False,
                    depends: DependsArg = (),
                    env: dict[str, str] | None = None,
                    order_only_deps: DependsArg = (),
                    disabled_reason: str | None = None) -> Task[ProcessOutputResult]:
    """
    Declare a task that executes the given command. See
    :func:`.def_task` and :func:`.Context.run` for
    parameter information.

    :return: The :class:`.Task` that was created
    """
    cmd = list(cmd)
    assert cmd, '`cmd` must not be empty for declare_cmd_task'

    async def _command_runner() -> ProcessOutputResult:
        handler: LineHandler
        if output in ('silent', 'accumulate'):
            handler = lambda b: None
        elif output == 'live':
            handler = LogLine()
        else:
            assert output == 'status', f'Invalid output={output!r}"'
            handler = UpdateStatus('')
        return await run_proc(
            cmd,
            cwd=cwd,
            check=check,
            env=env,
            on_line=handler,
        )

    dag = dag or current_dag()

    if isinstance(depends, (Task, str)):
        depends = [depends]

    depends_ = _iter_deps(depends, order_only_deps)

    t = Task[ProcessOutputResult](name=name,
                                  fn=_command_runner,
                                  depends=depends_,
                                  default=default,
                                  doc=doc,
                                  disabled_reason=disabled_reason)
    dag.add_task(t)
    return t
