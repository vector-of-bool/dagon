from __future__ import annotations
from typing import Iterable, NamedTuple, Sequence
import textwrap
import itertools
import shlex

from . import ansi


class ProcessResultUIInfo(NamedTuple):
    command: Sequence[str]
    retcode: int
    stdout: bytes
    stderr: bytes


def _make_label_pair(label: str, content: str, width: int) -> Iterable[str]:
    left = f'{label}: '
    lines = textwrap.wrap(content, width=width, initial_indent=left, subsequent_indent=' ' * len(left))
    for l in lines:
        line = f'| {l: <{width}} │'
        yield line


def _print_boxed_output(label: str, content: str, width: int) -> Iterable[str]:
    yield f'├{f" {label} ":─^{width+2}}┤'
    for l in content.splitlines():
        l = ansi.strip_escapes(l)
        for wl in textwrap.wrap(l, width=width, subsequent_indent=' ⏎ ', drop_whitespace=False):
            yield f'│ {wl: <{width}} │'


def _box_lines(result: ProcessResultUIInfo, max_width: int) -> Iterable[str]:
    cmd_plain = ' '.join(shlex.quote(s) for s in result.command)
    max_out = max((len(l) for l in result.stdout.splitlines()), default=0)
    max_err = max((len(l) for l in result.stderr.splitlines()), default=0)
    max_cmd = len(cmd_plain) + 10
    inner_width = max(max_out, max_err, max_cmd, 10)
    inner_width = min(inner_width, max_width - 4)
    # The top of the box:
    top = f'╒{"":═<{inner_width+2}}╕'
    yield top
    # Labels:
    pairs = (
        ('Command', cmd_plain),
        (' Exited', str(result.retcode)),
    )
    yield from itertools.chain.from_iterable(_make_label_pair(label, txt, inner_width) for label, txt in pairs)
    # The actual output:
    if result.stdout:
        yield from _print_boxed_output('stdout', result.stdout.decode('utf-8', 'surrogateescape'), inner_width)
    if result.stderr:
        yield from _print_boxed_output('stderr', result.stderr.decode('utf-8', 'surrogateescape'), inner_width)
    yield f'└{"":─<{inner_width+2}}┘'


def make_proc_info_box(result: ProcessResultUIInfo, max_width: int | None = None) -> str:
    return '\n'.join(_box_lines(result, max_width or 9999999))
