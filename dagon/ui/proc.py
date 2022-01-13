from __future__ import annotations

import itertools
import shlex
import textwrap
from typing import Iterable, NamedTuple, Sequence

from typing_extensions import Literal, Protocol

from dagon.ui.message import Message, MessageType


class _ProcOutputItem(Protocol):
    out: bytes
    kind: Literal['error', 'output']


class ProcessResultUIInfo(NamedTuple):
    command: Sequence[str]
    retcode: int
    output: Sequence[_ProcOutputItem]


def _make_label_pair(label: str, content: str, width: int) -> Iterable[Message]:
    left = f'{label}: '
    lines = textwrap.wrap(content, width=width - 1, initial_indent=left, subsequent_indent=' ' * len(left))
    for l in lines:
        line = f'│ {l: <{width}} │'
        yield Message(line, MessageType.MetaPrint)


def _print_boxed_output(output: Iterable[_ProcOutputItem], width: int) -> Iterable[Message]:
    yield Message(f'└{f" Output: ":─<{width+2}}┘', MessageType.MetaPrint)
    for item in output:
        l = item.out.decode(errors='surrogateescape').rstrip()
        if item.kind == 'error':
            yield Message(l, MessageType.Error)
        else:
            yield Message(l, MessageType.Print)


def _box_lines(result: ProcessResultUIInfo, max_width: int) -> Iterable[Message]:
    cmd_plain = ' '.join(shlex.quote(s) for s in result.command)
    max_cmd = len(cmd_plain) + 10
    max_line = max((len(l.out) for l in result.output), default=0) - 5
    inner_width = max(max_cmd, max_line, 10)
    inner_width = min(inner_width, max(10, max_width - 15))
    # The top of the box:
    top = f'╒{"":═<{inner_width+2}}╕'
    yield Message(top, MessageType.MetaPrint)
    # Labels:
    pairs = (
        ('Command', cmd_plain),
        (' Exited', str(result.retcode)),
    )
    yield from itertools.chain.from_iterable(_make_label_pair(label, txt, inner_width) for label, txt in pairs)
    # The actual output:
    yield from _print_boxed_output(result.output, inner_width)
    yield Message(f'└{" End of output ":─<{inner_width+2}}┘', MessageType.MetaPrint)


def make_proc_info_box(result: ProcessResultUIInfo, max_width: int | None = None) -> Iterable[Message]:
    return _box_lines(result, max_width or 9999999)
