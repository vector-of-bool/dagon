"""
Support for ANSI control sequences
"""
from __future__ import annotations
import re
from typing import cast

import os
import sys

ESC = '\x1b'
CSI = ESC + '['
NORMAL = CSI + '0m'
INVERSE = CSI + '7m'
NO_INVERSE = CSI + '27m'


def is_ansi_supported(fileno: int | None = None) -> bool:
    if fileno is None:
        fileno = sys.__stdout__.fileno()

    if not os.isatty(fileno):
        return False

    if os.name != 'nt':
        # Nix platforms support ANSI on TTYs by default
        return True

    import msvcrt  # pylint: disable=import-error
    from ctypes import byref, windll  # type: ignore
    from ctypes.wintypes import DWORD
    try:
        handle = msvcrt.get_osfhandle(fileno)  # type: ignore
    except OSError as e:
        if e.errno != 9:
            raise
        # Failed to get a HANDLE for the given fileno
        return False

    mode = DWORD()
    ok = windll.kernel32.GetConsoleMode(handle, byref(mode))
    assert ok, windll.kernel32.GetLastError()
    return (mode.value & 0x4) != 0


def ensure_ansi_term(fileno: int | None = None) -> bool:
    if not fileno:
        fileno = sys.__stdout__.fileno()

    if os.name != 'nt':
        # We only need to do this work for WIndows
        return is_ansi_supported(fileno)

    import msvcrt  # pylint: disable=import-error
    from ctypes import byref, windll  # type: ignore
    from ctypes.wintypes import DWORD
    try:
        handle = msvcrt.get_osfhandle(fileno)  # type: ignore
    except OSError as e:
        if e.errno != 9:
            raise
        # Failed to get a HANDLE for the given fileno
        return False

    mode = DWORD()
    ok = windll.kernel32.GetConsoleMode(handle, byref(mode))
    assert ok, windll.kernel32.GetLastError()
    new_mode = int(mode.value) | 0x4
    ok = windll.kernel32.SetConsoleMode(handle, new_mode)
    return bool(ok)


def style(style_code: str, string: str, force: bool = False) -> str:
    if not force and not os.isatty(sys.__stdout__.fileno()):
        return string
    return f'{CSI}{style_code}m{string}{NORMAL}'


def bold_green(s: str) -> str:
    return style('1;32', s)


def bold_red(s: str) -> str:
    return style('1;31', s)


def bold_cyan(s: str) -> str:
    return style('1;36', s)


def bold_yellow(s: str) -> str:
    return style('1;33', s)


def bold(s: str) -> str:
    return style('1', s)


def get_term_width() -> int:
    import struct
    if os.name == 'nt':
        # Windows has a special way to get the console dims
        from ctypes import create_string_buffer, windll  # type: ignore
        handle = windll.kernel32.GetStdHandle(-11)  # -11 == STD_OUTPUT_HANDLE
        buf = create_string_buffer(22)
        okay = windll.kernel32.GetConsoleScreenBufferInfo(handle, buf)
        if not okay:
            return 80
        info = struct.unpack('hhhhHhhhhhh', buf.raw)
        return cast(int, info[7] - info[5])

    import termios
    from fcntl import ioctl
    buf2 = ioctl(sys.__stdout__, termios.TIOCGWINSZ, b'\000' * 8)
    width: int = struct.unpack('hhhh', buf2)[1]
    return width


ESCAPE_RE = re.compile(r'\x1b(\[|\()[0-?]*[ -/]*[@-~]')


def strip_escapes(s: str) -> str:
    return ESCAPE_RE.sub('', s)
