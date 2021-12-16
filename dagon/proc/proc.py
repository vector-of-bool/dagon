"""
Module for dealing with subprocesses
"""

from __future__ import annotations

import datetime
import asyncio
import json
import os
import signal
import subprocess
import warnings
from pathlib import Path, PurePath
from typing import (Callable, Any, Iterable, Mapping, NamedTuple, Optional, Sequence, Union, cast)

from typing_extensions import Literal, Protocol

from ..event.cancel import CancellationToken, CancelLevel, raise_if_cancelled
from ..fs import Pathish

_AioProcess = asyncio.subprocess.Process  # pylint: disable=no-member

LineHandler = Callable[[bytes], None]
# mypy does not yet support recursively defined types.
CommandArg = Union[Pathish, str, int, float]
CommandLineElement = Union[CommandArg, Iterable['CommandLineElement']]
CommandLine = Iterable[CommandLineElement]

ProcessCompletionCallback = Callable[['RunningProcess', 'ProcessResult'], None]

OutputMode = Literal['accumulate', 'silent', 'live', 'status']


class ProcessResult(Protocol):
    @property
    def retcode(self) -> int:
        "The exit code of the process"
        ...

    @property
    def stdout(self) -> bytes | None:
        "The output of the process"
        ...

    @property
    def stderr(self) -> bytes | None:
        "The error output of the process"
        ...

    def stdout_json(self) -> Any:
        "Interpret the output as JSON data. Raises if stdout is None."
        ...


class ProcessOutputResult(Protocol):
    @property
    def retcode(self) -> int:
        "The exit code of the process"
        ...

    @property
    def stdout(self) -> bytes:
        "The output of the process"
        ...

    @property
    def stderr(self) -> bytes:
        "The error output of the process"
        ...

    def stdout_json(self) -> Any:
        "Interpret the output as JSON data."
        ...


class ProcessResultTup(NamedTuple):
    retcode: int
    stdout: Optional[bytes]
    stderr: Optional[bytes]

    def stdout_json(self) -> Any:
        if self.stdout is None:
            raise RuntimeError('Cannot interpret null process stdout as JSON data')
        return json.loads(self.stdout.decode())


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
                 line_handler: Optional[LineHandler] = None,
                 on_done: Optional[ProcessCompletionCallback] = None,
                 timeout: Optional[datetime.timedelta] = None) -> None:
        self._loop = loop
        self._pipe = proc
        self._line_handler = line_handler
        self._on_done = on_done

        self._stderr: 'asyncio.Future[Optional[bytes]]' = loop.create_task(self._read(proc.stderr))
        self._stdout: 'asyncio.Future[Optional[bytes]]' = loop.create_task(self._read(proc.stdout))
        self._result: asyncio.Task[ProcessResult] = loop.create_task(self._wait_result())
        self._timer: Optional[asyncio.TimerHandle] = None
        if timeout is not None:
            loop.call_later(timeout.total_seconds(), self._timeout)
        self._done = False
        self._cancelled = False
        self._timed_out = False

    async def _read(self, pipe: Optional[asyncio.StreamReader]) -> Optional[bytes]:
        if pipe is None:
            return None
        if self._line_handler is None:
            return await pipe.read()

        acc = b''
        line_acc = b''
        while 1:
            data = await pipe.read(1024)
            line_acc += data
            acc += data
            while True:
                nl_offset = line_acc.find(b'\n')
                if nl_offset < 0:
                    break
                line, line_acc = line_acc[:nl_offset + 1], line_acc[nl_offset + 1:]
                self._line_handler(line)
            if not data:
                break
        if line_acc:
            # Flush any remaining bytes that may not have been followed by a newline
            self._line_handler(line_acc)
        return acc

    async def _wait_result(self) -> ProcessResult:
        rc = await self._pipe.wait()
        out = await self._stdout
        err = await self._stderr
        self._done = True
        result = ProcessResultTup(rc, out, err)
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

    def send_signal(self, sig: int, to_pgrp: int = False) -> None:
        """
        Send a signal to the process or its process group.

        :param sig: The signal number to send.
        :param to_pgrp: If ``True``, sends the signal to the process group. The
            process should be a group leader. This is only supported on
            Unix-like systems.
        """
        if to_pgrp:
            os.killpg(self._pipe.pid, sig)
        else:
            self._pipe.send_signal(sig)

    def mark_cancelled(self) -> None:
        """
        Mark the process as 'cancelled'. This will cause the process result to
        raise an :class:`asyncio.CancelledError` regardless of the result of
        the subprocess. Note that this method does not interact with the
        subprocess in any way.
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

    async def communicate(self, dat: Union[None, str, bytes] = None) -> ProcessResult:
        """
        Send data to the subprocess's standard input stream and wait for the
        process to exit.

        :param dat: The input data. If given a ``str``, it will be encoded with
            UTF-8 before being sent. If not given, no data will be written to
            the standard input stream. If this is not ``None``, then the
            process should have been opened with ``stdin`` set to be a pipe. If
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

    This function may look dumb, but it ensures types better than just calling
    `str` regularly, which accepts anything as an argument.

    The following types are supported as command-line elements:

    - :class:`~pathlib.PurePath` and its subclasses (including
      :class:`~pathlib.Path`)
    - :class:`int`
    - :class:`float`
    - :class:`str`
    """
    return str(item)


def flatten_cmdline(cmd: CommandLine) -> Iterable[CommandArg]:
    """
    Dagon supports nested sequences as command lines, but the subprocess APIs
    do not. This API will flatten such nested sequences into a flat iterable.

    :param cmd: The iterable of command line arguments, possibly nested.

    .. note::
        It is not necessary to call this function when passing command lines to
        most Dagon APIs, as they will call this function themselves.

    :raises: :class:`TypeError` if any of the elements of the iterables is not
        a valid command-line element that Dagon supports.
        See :func:`arg_to_string`.
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
    Convert a Dagon-supported nested command-line object into a :class:`list`
    of just :class:``str`` objects.
    """
    if isinstance(cmd, str):
        raise TypeError('You have tried to spawn a process using '
                        'a string as the command. Commands are not '
                        'strings! Pass a list of command line arguments '
                        'instead. NOTE: Shell-isms are NOT supported.')
    return [arg_to_string(s) for s in flatten_cmdline(cmd)]


def get_effective_env(env: Optional[Mapping[str, str]], *, merge: bool = True) -> dict[str, str]:
    if merge:
        final_env = os.environ.copy()
    else:
        assert env is not None, 'get_env() requires an `env` argument when merge==True'
        final_env = dict(env)
    if env and merge:
        final_env.update(env)
    return final_env


def _record_proc(ctx: TaskContext, cmd: Sequence[str], start_time: datetime.datetime, cwd: Path,
                 result: ProcessResult) -> None:
    iv = ctx.db.new_interval(ctx.task_run_id, f'Subprocess {cmd}', start_time)
    end_time = datetime.datetime.now()
    dur = end_time - start_time
    pid = ctx.db.store_proc_execution(
        ctx.task_run_id,
        cmd=cmd,
        cwd=cwd,
        stdout=result.stdout,
        stderr=result.stderr,
        retc=result.retcode,
        start_time=start_time,
        duration=dur.total_seconds(),
    )
    meta = {'process_id': pid}
    ctx.db.set_interval_meta(iv, meta)
    ctx.db.set_interval_end(iv, end_time)


def _proc_done(cmd: Sequence[str], start_time: datetime.datetime, cwd: Path, record: bool, result: ProcessResult,
               proc: RunningProcess, next_handler: ProcessCompletionCallback) -> None:
    ctx = None
    if ctx and record:
        # Store this process execution in the database
        _record_proc(ctx, cmd, start_time, cwd, result)
    next_handler(proc, result)


async def spawn(cmd: CommandLine,
                *,
                cwd: Optional[Pathish] = None,
                env: Optional[Mapping[str, str]] = None,
                merge_env: bool = True,
                stdin_pipe: bool = False,
                on_line: Optional[LineHandler] = None,
                on_done: Optional[ProcessCompletionCallback] = None,
                record: bool = True,
                cancel: Optional[CancellationToken] = None,
                timeout: Optional[datetime.timedelta] = None) -> RunningProcess:
    """
    Spawn a subprocess and return a handle to that process.

    :param cmd: The command-line used to spawn the subprocess. Dagon supports
        nested iterables as command-line objects, which will be flattened into
        a list of strings before being used to spawn the process. Refer to
        :func:`flatten_cmdline` and :func:`arg_to_string`.
    :param cwd: The working directory for the subprocess. If not provided, will
        inherit the working directory of the Python process.
    :param env: Set the environment variables for the subprocess. **Note** that
        this will *replace* the environment variables, not append to the
        environment it would inherit from the parent process.
    :param stdin_pipe: If ``True``, a standard-input stream will be opened for
        the process and available on the returned :class:`RunningProcess`
        object. If ``False``, no standard-input stream will be given to the
        process.
    :param line_handler: A function that will be called for each line of output
        produced by the subprocess.
    :param on_done: A callback that will be invoked when the subprocess exits.
    :param cancel: A cancellation token for the child process. If a
        cancellation event is received, a ``SIGINT`` signal will be sent to
        the process (as if the user pressed :kbd:`ctrl+c`)
    """
    if isinstance(cmd, str):
        raise TypeError('You have tried to spawn a process using '
                        'a string as the command. Commands are not '
                        'strings! Pass a list of command line arguments '
                        'instead. NOTE: Shell-isms are NOT supported.')
    raise_if_cancelled(cancel)
    # Flatten the command line
    cmd_plain = plain_commandline(cmd)
    # Normalize to the actuall working dir
    cwd = Path(cwd or Path.cwd())
    # Get the subprocess environment
    env = get_effective_env(env, merge=merge_env)
    loop = asyncio.get_event_loop()
    # Get the cancellation token that may be in the task context
    cancel = cancel or CancellationToken.get_context_local()
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
    # Call _proc_done when the process completes
    on_done = lambda p0, p1: _proc_done(cmd_plain, start_time, cwd_, record, p1, p0, prev_on_done)

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


class UpdateStatus:
    def __init__(self, prefix: str) -> None:
        self._prefix = prefix

    def __call__(self, line: bytes) -> None:
        from dagon import ui
        ui.status(self._prefix + ' ' + line.decode(errors='?'))


class LogLine:
    def __call__(self, line: bytes) -> None:
        print(line.decode(errors='?'), end='', flush=True)
        return
        from dagon import ui
        ui.print(line.decode(errors='?'))


async def run(cmd: CommandLine,
              *,
              cwd: Optional[Pathish] = None,
              env: Optional[Mapping[str, str]] = None,
              merge_env: bool = True,
              stdin: Union[None, str, bytes] = None,
              check: bool = True,
              on_line: Optional[LineHandler] = None,
              on_done: Optional[ProcessCompletionCallback] = None,
              record: bool = True,
              cancel: Optional[CancellationToken] = None,
              timeout: Optional[datetime.timedelta] = None) -> ProcessOutputResult:
    """
    Execute a subprocess and wait for the result.

    :param check: If ``True``, the exit code of the process will be checked. If
        it is non-zero, then this function will raise a
        :class:`CalledProcessError` exception with the output information for
        the subprocess.
    :param stdin: A ``bytes`` or ``str`` object. A ``str`` object will be
        encoded as UTF-8 ``bytes`` object. The ``bytes`` will be sent to the
        standard-input stream of the subprocess. If ``stdin`` is not provided,
        the subprocess will not have a standard-input stream.

    See :func:`.spawn` for more parameter information.
    """
    proc = await spawn(
        cmd,
        cwd=cwd,
        env=env,
        merge_env=merge_env,
        stdin_pipe=stdin is not None,
        on_line=on_line,
        on_done=on_done,
        record=record,
        cancel=cancel,
        timeout=timeout,
    )
    result = await proc.communicate(stdin)
    if check:
        if result.retcode != 0:
            raise subprocess.CalledProcessError(
                result.retcode,
                plain_commandline(cmd),
                output=result.stdout,
                stderr=result.stderr,
            )
    assert result.stdout is not None
    assert result.stderr is not None
    return cast(ProcessOutputResult, result)