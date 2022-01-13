"""
Classes and helpers for presenting Dag run information to the user.
"""

from __future__ import annotations

import asyncio
import contextvars
import os
import sys
import textwrap
from dataclasses import dataclass
from collections import OrderedDict
from contextlib import AsyncExitStack, ExitStack, asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, AsyncIterator, Iterator, NamedTuple, Optional
from dagon.core.result import Failure, NodeResult, Success

from dagon.ext.iface import OpaqueTaskGraphView
from dagon.task.dag import OpaqueTask
from dagon.ui.events import ProgressInfo, UIEvents
from dagon.ui.message import Message, MessageType
from dagon.ui.proc import ProcessResultUIInfo, make_proc_info_box

from . import ansi
from .iface import I_UIExtension
from .stdio_cap import CapturedOutput, OutputAccumulator, capture_std_output
from ..util import scope_set_contextvar, typecheck

_MOVE_UP = ansi.CSI + 'A'
_MOVE_DOWN = '\n'  # ansi.CSI + 'B'
_GOTO_BOL = ansi.CSI + '1G'
_CLEAR_EOS = ansi.CSI + 'J'
_CLEAR_EOL = ansi.CSI + 'K'
_HIDE_CURSOR = ansi.CSI + '?25l'
_SHOW_CURSOR = ansi.CSI + '?25h'


@dataclass()
class _GlobalContext:
    n_total: int
    n_pending: int
    n_running: int
    n_failed: int
    n_succeeded: int
    n_cancelled: int
    stdio_cap: CapturedOutput
    stderr_acc: OutputAccumulator
    stdout_acc: OutputAccumulator


class _TaskContext(NamedTuple):
    task: OpaqueTask
    label: _FancyLabel


_GLOBAL_CTX = contextvars.ContextVar[_GlobalContext]('_GLOBAL_CTX')
_TASK_CTX = contextvars.ContextVar[_TaskContext]('_TASK_CTX')


class _FancyLabel:
    def __init__(self, name: str) -> None:
        self._name = name
        self.content: str = ''
        self.progress: Optional[float] = None

    @property
    def name(self) -> str:
        """The name string for the label"""
        return self._name


class FancyUI:
    dagon_ui_name = 'dagon.ui.fancy'
    dagon_ui_opt_name = 'fancy'

    # pylint: disable=too-many-instance-attributes
    def __init__(self) -> None:
        super().__init__()
        self._labels: dict[str, _FancyLabel] = OrderedDict()
        self._redraw_pending = False
        self._pending_output = ''
        self._pending_write = ''
        self._out_wrapper = textwrap.TextWrapper(replace_whitespace=False, drop_whitespace=False)

    @asynccontextmanager
    async def ui_global_context(self, graph: OpaqueTaskGraphView, events: UIEvents) -> AsyncIterator[None]:
        ansi.ensure_ansi_term()
        n_total = len(list(graph.all_nodes))
        async with AsyncExitStack() as st:
            cap = st.enter_context(capture_std_output())
            ctx = _GlobalContext(
                n_total=n_total,
                n_pending=n_total,
                n_running=0,
                n_failed=0,
                n_succeeded=0,
                n_cancelled=0,
                stdio_cap=cap,
                stderr_acc=OutputAccumulator(),
                stdout_acc=OutputAccumulator(),
            )
            loop = asyncio.get_event_loop()
            st.enter_context(scope_set_contextvar(_GLOBAL_CTX, ctx))
            st.callback(self._clear_on_exit)
            st.enter_context(self._hidden_cursor())
            st.enter_context(self._disable_term_echo())
            st.enter_context(events.message.connect(self._on_message))
            st.enter_context(events.progress.connect(self._on_progress))
            st.enter_context(events.task_result.connect(self._on_task_result))
            st.enter_context(events.status.connect(self._on_status))
            st.enter_context(cap.on_out.connect(lambda s: loop.call_soon_threadsafe(lambda: self._on_std_out(s))))
            st.enter_context(cap.on_err.connect(lambda s: loop.call_soon_threadsafe(lambda: self._on_std_out(s))))
            st.enter_context(events.process_done.connect(self._on_proc_done))
            self._redraw()
            yield

    @asynccontextmanager
    async def ui_task_context(self, task: OpaqueTask) -> AsyncIterator[None]:
        glb = _GLOBAL_CTX.get()
        glb.n_running += 1
        glb.n_pending -= 1
        label = self._labels[task.name] = _FancyLabel(task.name)
        with ExitStack() as st:
            st.enter_context(scope_set_contextvar(_TASK_CTX, _TaskContext(task, label)))
            label.content = 'Running…'
            self._schedule_redraw()
            yield
        self._labels.pop(task.name)

    def _write(self, b: str) -> None:
        self._pending_write += b

    def _commit(self) -> None:
        if os.name == 'nt':
            self._commit_win()
        else:
            self._commit_posix()

    def _commit_win(self) -> None:
        glb = _GLOBAL_CTX.get()
        glb.stdio_cap.real_stdout.write(self._pending_write)
        self._pending_write = ''

    def _commit_posix(self) -> None:
        glb = _GLOBAL_CTX.get()
        fd = glb.stdio_cap.real_stdout.fileno()
        buf = self._pending_write.encode()
        while buf:
            try:
                n_written = os.write(fd, buf)
            except BlockingIOError:
                continue
            else:
                buf = buf[n_written:]

        glb.stdio_cap.real_stdout.flush()
        self._pending_write = ''

    def _schedule_redraw(self) -> None:
        # Don't schedule multiple redraws in the same pass around the ev loop
        if not self._redraw_pending:
            asyncio.get_event_loop().call_soon(self._redraw)
        self._redraw_pending = True

    def _flush_output_lines(self) -> None:
        out = self._pending_output
        self._pending_output = ''
        self._out_wrapper.width = ansi.get_term_width() + 1
        for line in out.splitlines(keepends=True):
            if '\n' not in line:
                self._pending_output = line
                break
            wrapped_lines = self._out_wrapper.wrap(line)
            for idx, wline in enumerate(wrapped_lines):
                # Insert a blank line above and write the line into it
                self._write(f'{ansi.CSI}L{wline}{_GOTO_BOL}')
                if idx + 1 < len(wrapped_lines):
                    self._write(_MOVE_DOWN)

    def _header_text(self) -> str:
        glb = _GLOBAL_CTX.get()
        items: list[str] = []
        if glb.n_running:
            items.append(f'{glb.n_running} running')
        if glb.n_succeeded:
            items.append(ansi.bold_green(f'{glb.n_succeeded} done'))
        if glb.n_failed:
            items.append(ansi.bold_red(f'{glb.n_failed} failed'))
        if glb.n_pending:
            items.append(f'{glb.n_pending} pending')

        return ', '.join(items) + f' ({glb.n_total} total)'

    def _redraw(self) -> None:
        term_width = ansi.get_term_width()
        self._redraw_pending = False
        n_lines = len(self._labels)
        self._flush_output_lines()

        restore_height = n_lines + 2
        self._write(_MOVE_DOWN)
        max_name_width = max((len(l.name) for l in self._labels.values()), default=0)
        # Create the header
        header = self._header_text()
        header_plain = ansi.strip_escapes(header)
        header_vis_len = len(header_plain)
        vis_len_diff = len(header) - header_vis_len
        adj_term_len = term_width + vis_len_diff
        header = f'╒{f" {header} ":═^{(adj_term_len - 2)}}╕'
        bar_offset = max_name_width + 3
        header = header[:bar_offset] + '╤' + header[bar_offset + 1:]
        self._write(ansi.NORMAL + header)
        # Write the status labels
        avail_status_width = (term_width - max_name_width) - 6
        for label in self._labels.values():
            self._write(_MOVE_DOWN)
            self._write(_CLEAR_EOL)
            line = f'│ {label.name:<{max_name_width}} │ '
            progress = label.progress or 0.0
            n_progress_chars = round((avail_status_width - 1) * progress)
            st_text = label.content
            st_text = ansi.strip_escapes(st_text)
            st_text = textwrap.shorten(st_text, avail_status_width, placeholder='[…]')
            full_text = f'{st_text: <{avail_status_width}}'
            inverted = full_text[:n_progress_chars]
            normal = full_text[n_progress_chars:]
            full_text = f'{ansi.INVERSE}{inverted}{ansi.NO_INVERSE}{normal}'
            line = line + full_text + '│'
            self._write(line)
            self._write(ansi.NORMAL)
        # Write the bottom bar
        self._write(_GOTO_BOL)
        self._write(_MOVE_DOWN)
        bot = '└' + ('─' * (term_width - 2)) + '┘'
        bot = bot[:bar_offset] + '┴' + bot[bar_offset + 1:]
        self._write(bot)
        # Restore cursor
        self._write(_MOVE_UP * restore_height)
        self._write(_GOTO_BOL)

        self._commit()

    @contextmanager
    def _hidden_cursor(self) -> Iterator[None]:
        self._write(_HIDE_CURSOR)
        try:
            yield
        finally:
            self._write(_SHOW_CURSOR)

    @staticmethod
    @contextmanager
    def _disable_term_echo() -> Iterator[None]:
        if os.name == 'nt':
            yield
            return
        import termios
        term_attrs = termios.tcgetattr(sys.stdin.fileno())
        # Fourth element are the flags we want to modify
        prev_lfags = term_attrs[3]
        term_attrs[3] &= ~termios.ECHO
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, term_attrs)
        try:
            yield
        finally:
            term_attrs[3] = prev_lfags
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, term_attrs)

    def _clear_on_exit(self) -> None:
        # Forcibly do the final draw before reverting stdout
        self._redraw()
        self._write(_CLEAR_EOS)
        self._commit()

    def _on_task_result(self, what: NodeResult[OpaqueTask]) -> None:
        glb = _GLOBAL_CTX.get()
        glb.n_running -= 1
        if isinstance(what.result, Failure):
            glb.n_failed += 1
        elif isinstance(what.result, Success):
            glb.n_succeeded += 1
        self._write(_CLEAR_EOS)
        self._schedule_redraw()

    def _on_std_out(self, s: str) -> None:
        self._pending_output += s
        self._redraw()

    def _append_message(self, msg: Message) -> None:
        text = msg.content
        if msg.type in (MessageType.Print, MessageType.MetaPrint):
            if msg.type == MessageType.MetaPrint:
                text = ansi.bold_cyan(text)
            self._pending_output += text + '\n'
            self._schedule_redraw()
            return
        # Other logging messages should be drawn immediately with color:
        col = {
            MessageType.Error: ansi.bold_red,
            MessageType.Fatal: ansi.bold_red,
            MessageType.Warning: ansi.bold_yellow,
            MessageType.Information: ansi.bold_cyan,
        }.get(msg.type, ansi.bold_cyan)
        self._pending_output += col(text) + '\n'

    def _on_message(self, msg: Message) -> None:
        self._append_message(msg)
        # Redraw the screen immediately
        self._redraw()

    def _on_status(self, s: str) -> None:
        _TASK_CTX.get().label.content = s
        self._schedule_redraw()

    def _on_progress(self, pr: ProgressInfo) -> None:
        ctx = _TASK_CTX.get()
        progress = pr.progress
        if progress:
            progress = round(progress, 2)
        lbl = self._labels[ctx.task.name]
        if lbl.progress == progress:
            return
        lbl.progress = progress
        self._schedule_redraw()

    def _on_proc_done(self, result: ProcessResultUIInfo) -> None:
        for m in make_proc_info_box(result, max_width=ansi.get_term_width()):
            self._append_message(m)
        self._schedule_redraw()


if TYPE_CHECKING:
    typecheck(I_UIExtension)(FancyUI)