from __future__ import annotations
import sys

from contextlib import contextmanager
import traceback
from typing import IO, Iterable, Iterator

from ..event import Event


class StdOutputPatch:
    def __init__(self, h: Event[str]) -> None:
        self._handle = h

    def write(self, dat: str) -> None:
        try:
            self._handle.emit(dat)
        except:
            tb = traceback.format_exc()
            print(f'Exception while writing through patched stdio stream: {tb}', file=sys.__stderr__)

    def flush(self) -> None:
        pass


class CapturedOutput:
    def __init__(self, prev_stdout: IO[str], prev_stderr: IO[str]) -> None:
        self._stdout = prev_stdout
        self._stderr = prev_stderr
        self._on_err = Event[str]()
        self._on_out = Event[str]()

    @property
    def real_stdout(self) -> IO[str]:
        """The captured standard output device"""
        return self._stdout

    @property
    def real_stderr(self):
        """The captured standard error device"""
        return self._stderr

    @property
    def on_out(self):
        """Event fired for captured stdout output"""
        return self._on_out

    @property
    def on_err(self):
        """Event fired for captured stderr output"""
        return self._on_err


@contextmanager
def capture_std_output() -> Iterator[CapturedOutput]:
    cap = CapturedOutput(sys.stdout, sys.stderr)
    sys.stdout = StdOutputPatch(cap.on_out)
    sys.stderr = StdOutputPatch(cap.on_err)
    try:
        yield cap
    finally:
        sys.stdout = cap.real_stderr
        sys.stderr = cap.real_stdout


class OutputAccumulator:
    def __init__(self) -> None:
        self._acc: bytes = b''

    def append(self, content: str | bytes) -> None:
        if isinstance(content, str):
            content = content.encode('utf-8')
        self._acc += content

    def take_lines(self) -> Iterable[bytes]:
        pos = self._acc.find(b'\n')
        while pos >= 0:
            part = self._acc[:pos + 1]
            yield part
            self._acc = self._acc[pos + 1:]
            pos = self._acc.find(b'\n')
