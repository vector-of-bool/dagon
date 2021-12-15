from __future__ import annotations

import sys
from contextlib import ExitStack, asynccontextmanager, contextmanager
from typing import AsyncIterator, Iterator, TextIO

from dagon.event import Event, events
from dagon.task.dag import OpaqueTask
from dagon.util import AsyncNullContext, unused

from .base import BaseExtension
from .iface import OpaqueTaskGraphView


class PrintCollector(BaseExtension[None, 'PrintCollector', None]):
    dagon_ext_name = 'dagon.print'
    dagon_ext_requires = ['dagon.events']

    def __init__(self):
        self._stdout: TextIO | None = None
        self._stderr: TextIO | None = None
        self.on_print = Event[str]()

    @contextmanager
    def _patch_stdout(self) -> Iterator[TextIO]:
        old_stdout = sys.stdout
        old_stderr = sys.stderr

        evh = self

        class StdIOPatch:
            def write(self, dat: str) -> None:  # pylint: disable=no-self-use
                # pylint: disable=protected-access
                evh.on_print.emit(dat)
                sys.__stdout__.write(dat)

            def flush(self) -> None:
                pass

        sys.stderr = sys.stdout = StdIOPatch()  # type: ignore
        try:
            self._stdout = old_stdout  # type: ignore
            yield old_stdout
        finally:
            self._stdout = None
            sys.stdout = old_stdout
            sys.stderr = old_stderr

    @asynccontextmanager
    async def global_context(self, graph: OpaqueTaskGraphView) -> AsyncIterator[PrintCollector]:
        unused(graph)
        with ExitStack() as st:
            st.enter_context(self._patch_stdout())
            yield self

    def task_context(self, task: OpaqueTask):
        events.register('dagon.print', self.on_print)
        return AsyncNullContext()
