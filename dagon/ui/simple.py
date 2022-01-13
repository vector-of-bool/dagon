import asyncio
import contextvars
import time
from contextlib import ExitStack, asynccontextmanager
from dataclasses import dataclass, field
from typing import IO, TYPE_CHECKING
from dagon.ui import ansi

from dagon.ui.events import UIEvents
from dagon.ui.message import Message, MessageType
from dagon.ui.proc import ProcessResultUIInfo, make_proc_info_box

from .. import util
from ..ext.iface import OpaqueTaskGraphView
from ..task.dag import OpaqueTask
from .iface import I_UIExtension
from .stdio_cap import CapturedOutput, OutputAccumulator, capture_std_output


@dataclass()
class _StdioAccum:
    capture: CapturedOutput
    stdout_acc: OutputAccumulator = field(default_factory=OutputAccumulator)
    stderr_acc: OutputAccumulator = field(default_factory=OutputAccumulator)


_GLBL_CTX = contextvars.ContextVar[_StdioAccum]('_CAPTURE_CTX')

_TASK_CTX = contextvars.ContextVar[OpaqueTask]('_TASK_CTX')


class SimpleUI:
    dagon_ui_name = 'dagon.ui.simple'
    dagon_ui_opt_name = 'simple'

    @asynccontextmanager
    async def ui_global_context(self, graph: OpaqueTaskGraphView, events: UIEvents):
        with ExitStack() as st:
            cap = st.enter_context(capture_std_output())
            st.enter_context(util.scope_set_contextvar(_GLBL_CTX, _StdioAccum(cap)))
            start = time.time()
            print(f'[dagon]: {len(tuple(graph.all_nodes))} tasks to run')
            loop = asyncio.get_event_loop()
            st.enter_context(cap.on_out.connect(lambda s: loop.call_soon_threadsafe(lambda: self._append_stdout(s))))
            st.enter_context(cap.on_err.connect(lambda s: loop.call_soon_threadsafe(lambda: self._append_stderr(s))))
            st.enter_context(events.message.connect(self._on_message))
            st.enter_context(events.status.connect(self._on_status))
            st.enter_context(events.process_done.connect(self._echo_proc))
            yield
            print(f'[dagon] Finished in {time.time() - start:.4}s')
            return

    @asynccontextmanager
    async def ui_task_context(self, task: OpaqueTask):
        print(f'[begin:{task.name}]')
        with util.scope_set_contextvar(_TASK_CTX, task):
            yield
        print(f'[end:{task.name}]')

    def _on_message(self, msg: Message) -> None:
        if msg.type != MessageType.Print:
            self._append_stderr(msg.content + '\n')
        else:
            self._append_stdout(msg.content + '\n')

    def _on_status(self, status: str) -> None:
        self._append_stdout(f'[status] {status}')

    def _echo_proc(self, result: ProcessResultUIInfo) -> None:
        for m in make_proc_info_box(result, max_width=ansi.get_term_width()):
            self._on_message(m)

    def _append_stdout(self, s: str) -> None:
        ctx = _GLBL_CTX.get()
        self._append(s, ctx.capture.real_stdout, ctx.stdout_acc)

    def _append_stderr(self, s: str) -> None:
        ctx = _GLBL_CTX.get()
        self._append(s, ctx.capture.real_stderr, ctx.stderr_acc)

    def _append(self, s: str, into: IO[str], acc: OutputAccumulator) -> None:
        acc.append(s)
        for l in acc.take_lines():
            task = _TASK_CTX.get(None)
            if task is not None:
                prefix = f'[dagon task:{task.name}] '.encode(errors='?')
            else:
                prefix = b'[dagon]'
            l = prefix + l
            into.write(l.decode(encoding='utf-8', errors='?'))


if TYPE_CHECKING:
    util.typecheck(I_UIExtension)(SimpleUI)
