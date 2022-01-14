"""
Module ``dagon.proc``
#####################

Subprocess utilities
"""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import signal
import subprocess
import warnings
from pathlib import Path, PurePath
from typing import (Any, Callable, Iterable, Mapping, NamedTuple, Sequence, Union, cast)

from typing_extensions import Literal, Protocol

from .. import db as db_mod
from .. import ui
from ..event.cancel import CancellationToken, CancelLevel, raise_if_cancelled
from ..fs import Pathish
from ..task import (DependsArg, Task, TaskDAG, current_dag, iter_deps)
from ..ui.message import MessageType

_AioProcess = asyncio.subprocess.Process

CommandArg = Union[Pathish, str, int, float]
CommandLineElement = Union[CommandArg, Iterable['CommandLineElement']]

CommandLine = Iterable[CommandLineElement]
"""
An arbitrarily nested process command. Each element must be a `str`, `int`,
`float`, `Pathish`, or iterable of `CommandLine` itself. When spawning a process,
`.flatten_cmdline` will be used to create a flat iterable of all elements.
"""

ProcessCompletionCallback = Callable[['RunningProcess', 'ProcessResult'], None]

OutputMode = Literal['print', 'status', 'silent', 'accumulate']


class ProcessOutputItem(NamedTuple):
    out: bytes
    kind: Literal['error', 'output']


LineHandler = Callable[[ProcessOutputItem], None]


class ProcessResult(Protocol):
    """
    The type of object resulting from a finished process execution with captured
    output.
    """
    @property
    def retcode(self) -> int:
        "The exit code of the process"
        ...

    @property
    def output(self) -> Sequence[ProcessOutputItem]:
        "The output records of the subprocess"
        ...

    def stdout_json(self) -> Any:
        "Interpret the output as JSON data."
        ...


class _ProcessResultTup(NamedTuple):
    retcode: int
    output: Sequence[ProcessOutputItem]

    def stdout_json(self) -> Any:
        dat = b''.join(i.out for i in self.output if i.kind == 'output')
        return json.loads(dat)


class RunningProcess:
    """
    Represents an executing subprocess controlled by :mod:`asyncio`.

    :param proc: The subprocess handle.
    :param loop: The event loop that owns the subprocess.
    :param line_handler: A callback that is invoked for each line of output
        generated by the subprocess.
    :param on_done: A callback that is invoked when the subprocess completes.

    .. note::
        Prefer to use :func:`.spawn` to create this object rather than creating
        one by hand.
    """
    def __init__(self,
                 *,
                 proc: _AioProcess,
                 loop: asyncio.AbstractEventLoop,
                 line_handler: LineHandler | None = None,
                 on_done: ProcessCompletionCallback | None = None,
                 timeout: datetime.timedelta | None = None) -> None:
        self._loop = loop
        self._pipe = proc
        self._line_handler = line_handler
        self._on_done = on_done

        assert proc.stderr
        assert proc.stdout
        self._output: list[ProcessOutputItem] = []
        self._stderr: asyncio.Future[None] = loop.create_task(self._read(proc.stderr, 'error'))
        self._stdout: asyncio.Future[None] = loop.create_task(self._read(proc.stdout, 'output'))
        self._result: asyncio.Task[ProcessResult] = loop.create_task(self._wait_result())
        self._timer: asyncio.TimerHandle | None = None
        if timeout is not None:
            self._timer = loop.call_later(timeout.total_seconds(), self._timeout)
        self._done = False
        self._cancelled = False
        self._timed_out = False

    async def _read(self, pipe: asyncio.StreamReader, kind: Literal['error', 'output']) -> None:
        if pipe is None:
            return None

        line_acc = b''
        while 1:
            data = await pipe.read(1024)
            line_acc += data
            while True:
                nl_offset = line_acc.find(b'\n')
                if nl_offset < 0:
                    break
                line, line_acc = line_acc[:nl_offset + 1], line_acc[nl_offset + 1:]
                item = ProcessOutputItem(line, kind)
                if self._line_handler:
                    self._line_handler(item)
                self._output.append(item)
            if not data:
                break
        if line_acc:
            # Flush any remaining bytes that may not have been followed by a newline
            item = ProcessOutputItem(line_acc, kind)
            if self._line_handler:
                self._line_handler(item)
            self._output.append(item)

    async def _wait_result(self) -> ProcessResult:
        rc = await self._pipe.wait()
        await self._stdout
        await self._stderr
        self._done = True
        if self._timer:
            self._timer.cancel()
        result = _ProcessResultTup(rc, self._output)
        if self._on_done:
            self._on_done(self, result)
        if self._timed_out:
            raise TimeoutError()
        if self._cancelled:
            raise asyncio.CancelledError()
        return result

    @property
    def result(self) -> 'asyncio.Future[ProcessResult]':
        """
        The process result. Awaiting on this object will block until the
        process exits.

        :rtype: asyncio.Future[ProcessResult]
        """
        return self._result

    def send_signal(self, sig: int, to_pgrp: int = True) -> None:
        """
        Send a signal to the process or its process group.

        :param sig: The signal number to send.
        :param to_pgrp: If `True`, sends the signal to the process group. The
            process should be a group leader. This is only supported on
            Unix-like systems.
        """
        if to_pgrp and not os.name == 'nt':
            os.killpg(self._pipe.pid, sig)
        else:
            self._pipe.send_signal(sig)

    def mark_cancelled(self) -> None:
        """
        Mark the process as 'cancelled'. This will cause the process result to
        raise an `asyncio.CancelledError` regardless of the result of
        the subprocess. Note that this method does not interact with the
        subprocess in any way.

        One should prefer to use a `.CancellationToken` to cancel a running
        process.
        """
        self._cancelled = True

    def _timeout(self) -> None:
        if self._done:
            return
        try:
            self.send_signal(signal.SIGINT, to_pgrp=True)
        except ProcessLookupError:
            return
        else:
            self._timed_out = True
            self._loop.call_later(10, self._timeout_terminate)

    def _timeout_terminate(self) -> None:
        if self._done:
            return
        try:
            self._pipe.terminate()
        except ProcessLookupError:
            return
        else:
            self._loop.call_later(10, self._timeout_kill)

    def _timeout_kill(self) -> None:
        if self._done:
            return
        try:
            self._pipe.kill()
        except ProcessLookupError:
            pass

    async def communicate(self, dat: None | str | bytes = None) -> ProcessResult:
        """
        Send data to the subprocess's standard input stream and wait for the
        process to exit.

        :param dat: The input data. If given a `str`, it will be encoded with
            UTF-8 before being sent. If not given, no data will be written to
            the standard input stream. If this is not `None`, then the
            process should have been opened with `stdin` set to be a pipe. If
            the process has already exited, then no data will be written.
        """
        if isinstance(dat, str):
            dat = dat.encode('utf-8')
        if self._done:
            warnings.warn('Writing to stdin of already finished process')
        if dat is not None:
            if self._pipe.stdin is None:
                warnings.warn('Cannot write data to process: stdin was not opened as a pipe')
            else:
                self._pipe.stdin.write(dat)
                await self._pipe.stdin.drain()
                self._pipe.stdin.close()
        return await self.result


def arg_to_string(item: CommandArg) -> str:
    """
    Convert a command argument to a string.

    This function may look unecessary, but it only accepts certain types,
    whereas `str` can be called with anything implementing `__str__`.

    The following types are supported as command-line elements:

    - `~pathlib.PurePath` and its subclasses (including
      `~pathlib.Path`)
    - `int`
    - `float`
    - `str`
    """
    return str(item)


def flatten_cmdline(cmd: CommandLine) -> Iterable[CommandArg]:
    """
    Dagon supports nested sequences as command lines, but the subprocess APIs
    do not. This API will flatten such nested sequences into a flat iterable.

    :param cmd: The iterable of command line arguments, possibly nested.

    :raises: `TypeError` if any of the elements of the iterables is not
        a valid command-line element that Dagon supports.
        See :func:`arg_to_string`.

    .. note::
        It is not necessary to call this function when passing command lines to
        most Dagon APIs, as they will call this function themselves.
    """
    for arg in cmd:
        if isinstance(arg, (str, int, float, PurePath)):
            yield arg
        elif hasattr(arg, '__fspath__'):
            yield cast(Pathish, arg)
        elif hasattr(arg, '__iter__'):
            yield from flatten_cmdline(arg)  # type: ignore
        else:
            raise TypeError(f'Invalid command line argument element {repr(arg)}')


def plain_commandline(cmd: CommandLine) -> list[str]:
    """
    Convert a Dagon-supported nested command-line object into a `list` of `str`.
    """
    if isinstance(cmd, str):
        raise TypeError('You have tried to spawn a process using '
                        'a string as the command. Commands are not '
                        'strings! Pass a list of command line arguments '
                        'instead. NOTE: Shell-isms are NOT supported.')
    return [arg_to_string(s) for s in flatten_cmdline(cmd)]


def get_effective_env(env: Mapping[str, str] | None, *, merge: bool = True) -> dict[str, str]:
    if merge:
        final_env = os.environ.copy()
    else:
        assert env is not None, 'get_env() requires an `env` argument when merge==True'
        final_env = dict(env)
    if env and merge:
        final_env.update(env)
    return final_env


def _record_proc(db: db_mod.Database, task_run_id: db_mod.TaskRunID, cmd: Sequence[str], start_time: datetime.datetime,
                 cwd: Path, result: ProcessResult) -> None:
    iv = db.new_interval(task_run_id, f'Subprocess {cmd}', start_time)
    end_time = datetime.datetime.now()
    dur = end_time - start_time
    pid = db.store_proc_execution(
        task_run_id,
        cmd=cmd,
        cwd=cwd,
        output=result.output,
        retc=result.retcode,
        start_time=start_time,
        duration=dur.total_seconds(),
    )
    meta = {'process_id': pid}
    db.set_interval_meta(iv, meta)
    db.set_interval_end(iv, end_time)


_PrintOnFinishArg = Literal['always', 'never', 'on-fail', None]


def _proc_done(cmd: Sequence[str], start_time: datetime.datetime, cwd: Path, record: bool,
               print_output_on_finish: _PrintOnFinishArg, result: ProcessResult, proc: RunningProcess,
               next_handler: ProcessCompletionCallback) -> None:
    gctx = db_mod.global_context_data()
    tctx = db_mod.task_context_data()
    if record and gctx and tctx:
        # Store this process execution in the database
        _record_proc(gctx.database, tctx.task_run_id, cmd, start_time, cwd, result)
    if (print_output_on_finish == 'always' or (print_output_on_finish == 'on-fail' and result.retcode != 0)):
        ui.process_done(ui.ProcessResultUIInfo(cmd, result.retcode, result.output))
    next_handler(proc, result)


async def spawn(cmd: CommandLine,
                *,
                cwd: Pathish | None = None,
                env: Mapping[str, str] | None = None,
                merge_env: bool = True,
                stdin_pipe: bool = False,
                on_output: OutputMode | LineHandler | None = None,
                on_done: ProcessCompletionCallback | None = None,
                print_output_on_finish: _PrintOnFinishArg = None,
                record: bool = True,
                cancel: CancellationToken | None = None,
                timeout: datetime.timedelta | None = None) -> RunningProcess:
    """
    Spawn a subprocess and return a handle to that process.

    :param cmd: The command-line used to spawn the subprocess. Dagon supports
        nested iterables as command-line objects, which will be flattened into
        a list of strings before being used to spawn the process. Refer to
        :func:`flatten_cmdline` and :func:`arg_to_string`.
    :param cwd: The working directory for the subprocess. If not provided, will
        inherit the working directory of the Python process.
    :param env: Set the environment variables for the subprocess. **Note** that
        if `merge_env` if `False`, this will *replace* the environment
        variables, not append to the environment it would inherit from the
        parent process.
    :param merge_env: If `True`, the value of `os.environ` will be copied and
        then merged with `env` to produce the environment given to the new
        process. This has no effect if `env` is `None`.
    :param stdin_pipe: If `True`, a standard-input stream will be opened for
        the process and available on the returned `RunningProcess`
        object. If `False`, no standard-input stream will be given to the
        process.
    :param on_line: A function that will be called for each line of output
        produced by the subprocess.
    :param on_done: A callback that will be invoked when the subprocess exits.
    :param timeout: A timeout for the subprocess execution. If the timeout is
        reached, then a series of signals will be sent to the subprocess: First
        `~signal.SIGINT`, then after grace period, `~signal.SIGTERM`, then after
        another grace period `~signal.SIGKILL`. After the timeout is reached,
        awaiting on the process result will raise `TimeoutError`.
    :param cancel: A cancellation token for the child process. If a
        cancellation event is received, a `~signal.SIGINT` signal will be sent
        to the process (as if the user pressed :kbd:`ctrl+c`), and awaiting on
        the process result will raise `~asyncio.CancelledError`
    """
    if isinstance(cmd, str):
        raise TypeError('You have tried to spawn a process using '
                        'a string as the command. Commands are not '
                        'strings! Pass a list of command line arguments '
                        'instead. NOTE: Shell-isms are NOT supported.')
    # Get the cancellation token that may be in the task context
    cancel = CancellationToken.resolve(cancel)
    raise_if_cancelled(cancel)
    # Flatten the command line
    cmd_plain = plain_commandline(cmd)
    # Normalize to the actuall working dir
    cwd = Path(cwd or Path.cwd())
    # Get the subprocess environment
    env = get_effective_env(env, merge=merge_env)
    loop = asyncio.get_event_loop()
    preexec_fn = None
    might_cancel = cancel is not None or timeout is not None
    if might_cancel and os.name != 'nt':
        # We might send a signal to this process via `cancel`, and we
        # don't want the controlling shell to send it `SIGINT` if the user
        # presses ^C.
        #
        # Make the process a session leader of a new session, which will have
        # no terminal and therefore no one to send it ^C.
        preexec_fn = os.setpgrp

    # Normalize on_done to a do-nothing handler
    on_done = on_done or (lambda p0, p1: None)
    prev_on_done = on_done
    start_time = datetime.datetime.now()
    cwd_ = cwd
    if print_output_on_finish is None:
        if on_output == 'accumulate':
            print_output_on_finish = 'always'
        else:
            print_output_on_finish = 'on-fail'
    # Call _proc_done when the process completes
    on_done = lambda p0, p1: _proc_done(cmd_plain, start_time, cwd_, record, print_output_on_finish, p1, p0,
                                        prev_on_done)

    on_line = as_line_handler(on_output)

    # Spawn that subprocess!
    proc = await asyncio.create_subprocess_exec(
        *cmd_plain,
        cwd=cwd,
        env=env,
        stdin=subprocess.PIPE if stdin_pipe else subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        preexec_fn=preexec_fn,
    )

    ret = RunningProcess(proc=proc, loop=loop, line_handler=on_line, on_done=on_done, timeout=timeout)
    if cancel is not None:

        def cancel_it(_lvl: CancelLevel) -> None:
            if os.name != 'nt':
                try:
                    ret.send_signal(signal.SIGINT, to_pgrp=True)
                except ProcessLookupError:
                    # The process is already dead. We're okay.
                    pass
            ret.mark_cancelled()

        token = cancel.connect(cancel_it)
        ce = cancel
        ret.result.add_done_callback(lambda _: ce.disconnect(token))
    return ret


class _UpdateStatus:
    def __call__(self, line: ProcessOutputItem) -> None:
        ui.status(line.out.decode(errors='?'))


class _LogLine:
    def __call__(self, line: ProcessOutputItem) -> None:
        ui.print(line.out.decode(errors='?'), type=MessageType.Error if line.kind == 'error' else MessageType.Print)


PRINT_OUTPUT: LineHandler = _LogLine()
UPDATE_STATUS_OUTPUT: LineHandler = _UpdateStatus()


async def run(cmd: CommandLine,
              *,
              cwd: Pathish | None = None,
              env: Mapping[str, str] | None = None,
              merge_env: bool = True,
              stdin: None | str | bytes = None,
              check: bool = True,
              on_output: LineHandler | OutputMode | None = None,
              on_done: ProcessCompletionCallback | None = None,
              print_output_on_finish: _PrintOnFinishArg = None,
              record: bool = True,
              cancel: CancellationToken | None = None,
              timeout: datetime.timedelta | None = None) -> ProcessResult:
    """
    Execute a subprocess and wait for the result.

    :param check: If `True`, the exit code of the process will be checked. If
        it is non-zero, then this function will raise a
        `CalledProcessError` exception with the output information for
        the subprocess.
    :param stdin: A `bytes` or `str` object. A `str` object will be
        encoded as UTF-8 `bytes` object. The `bytes` will be sent to the
        standard-input stream of the subprocess. If `stdin` is not provided,
        the subprocess will not have a standard-input stream.

    See :func:`.spawn` for more parameter information.
    """
    proc = await spawn(
        cmd,
        cwd=cwd,
        env=env,
        merge_env=merge_env,
        stdin_pipe=stdin is not None,
        on_output=on_output,
        on_done=on_done,
        print_output_on_finish=print_output_on_finish,
        record=record,
        cancel=cancel,
        timeout=timeout,
    )
    result = await proc.communicate(stdin)
    if check:
        if result.retcode != 0:
            stdout = b''.join(o.out for o in result.output if o.kind == 'output')
            stderr = b''.join(o.out for o in result.output if o.kind == 'error')
            raise subprocess.CalledProcessError(
                result.retcode,
                plain_commandline(cmd),
                output=stdout,
                stderr=stderr,
            )
    return result


def cmd_task(name: str,
             cmd: CommandLine,
             *,
             dag: TaskDAG | None = None,
             cwd: Pathish | None = None,
             doc: str = '',
             on_output: LineHandler | OutputMode | None = 'accumulate',
             print_output_on_finish: _PrintOnFinishArg = None,
             check: bool = True,
             default: bool = False,
             depends: DependsArg = (),
             env: dict[str, str] | None = None,
             order_only_depends: DependsArg = (),
             disabled_reason: str | None = None) -> Task[ProcessResult]:
    """
    Declare a task that executes the given command. See
    :func:`dagon.task.define` and :func:`run` for parameter information.

    :return: The `.Task` that was created
    """
    cmd = list(cmd)
    assert cmd, '`cmd` must not be empty for declare_cmd_task'

    async def _command_runner() -> ProcessResult:
        return await run(
            cmd,
            cwd=cwd,
            check=check,
            env=env,
            on_output=on_output,
            print_output_on_finish=print_output_on_finish,
        )

    dag = dag or current_dag()

    if isinstance(depends, (Task, str)):
        depends = [depends]

    depends_ = iter_deps(depends, order_only_depends)

    t = Task[ProcessResult](name=name,
                            fn=_command_runner,
                            depends=depends_,
                            default=default,
                            doc=doc,
                            disabled_reason=disabled_reason)
    dag.add_task(t)
    return t


def as_line_handler(l: LineHandler | OutputMode | None) -> LineHandler | None:
    if l is None:
        return None
    if callable(l):
        return l
    if l in ('accumulate', 'silent'):
        return None
    if l == 'print':
        return PRINT_OUTPUT
    if l == 'status':
        return UPDATE_STATUS_OUTPUT
    assert False, f'Invalid value for line_handler/on_output: {l!r}'
