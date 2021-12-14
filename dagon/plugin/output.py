from __future__ import annotations

import sys
from contextlib import ExitStack, contextmanager, nullcontext
from typing import Iterator, TextIO

from dagon.event import Event, events
from dagon.plugin.base import BasePlugin
from dagon.plugin.iface import OpaqueTaskGraphView
from dagon.task.dag import OpaqueTask
from dagon.util import unused


class PrintCollector(BasePlugin['PrintCollector', None]):
    dagon_plugin_name = 'dagon.print'
    dagon_plugin_requires = ['dagon.events']

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

    @contextmanager
    def global_context(self, graph: OpaqueTaskGraphView) -> Iterator[PrintCollector]:
        unused(graph)
        with ExitStack() as st:
            st.enter_context(self._patch_stdout())
            yield self

    def task_context(self, task: OpaqueTask):
        events.register('dagon.print', self.on_print)
        return nullcontext()
