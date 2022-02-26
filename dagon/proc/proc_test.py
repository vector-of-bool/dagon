import asyncio
import datetime
from typing import Iterable
from ..event.cancel import CancellationToken
import sys

import pytest

from dagon import proc


def _join_out(out: Iterable[proc.ProcessOutputItem]) -> bytes:
    return b''.join(o.out for o in out)


@pytest.mark.asyncio
async def test_run_process_stdout():
    result = await proc.run([sys.executable, '-c', 'print("Hello!", end="")'])
    assert _join_out(result.output) == b'Hello!'


@pytest.mark.asyncio
async def test_run_proc_fail():
    result = await proc.run([sys.executable, '-c', 'import sys; sys.exit(42)'], check=False)
    assert result.retcode == 42
    assert _join_out(result.output) == b''


@pytest.mark.asyncio
async def test_run_cancel_via_token():
    token = CancellationToken()
    pipe = await proc.spawn([sys.executable, '-c', 'import time; time.sleep(5)'], cancel=token)
    token.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pipe.result


@pytest.mark.asyncio
async def test_run_timeout():
    with pytest.raises(TimeoutError):
        await proc.run([sys.executable, '-c', 'import time; time.sleep(1)'], timeout=datetime.timedelta(milliseconds=5))


@pytest.mark.asyncio
async def test_run_no_timeout():
    await proc.run([sys.executable, '-c', 'import time; time.sleep(0.2)'], timeout=datetime.timedelta(seconds=1))


@pytest.mark.asyncio
async def test_run_empty_stdin():
    result = await proc.run([sys.executable, '-c', 'import sys; s = sys.stdin.read(); print(s)'],
                            timeout=datetime.timedelta(seconds=1),
                            check=False)
    assert result.retcode == 0
    assert _join_out(result.output) == proc.OS_PREFERRED_NEWLINE


@pytest.mark.asyncio
async def test_run_simple_stdin():
    result = await proc.run(
        [sys.executable, '-c', 'import sys; a = input("a:"); b = input("b:"); print("got:", a, b, sys.stdin.read())'],
        stdin='eggs\nbacon\n',
        timeout=datetime.timedelta(seconds=1),
        check=False)
    assert result.retcode == 0
    assert _join_out(result.output) == b'a:b:got: eggs bacon ' + proc.OS_PREFERRED_NEWLINE


@pytest.mark.asyncio
async def test_output_handler():
    acc = b''

    def handle(data: proc.ProcessOutputItem):
        nonlocal acc
        acc += data.out

    res = await proc.run([sys.executable, '-c', 'import sys; sys.stdout.write("abc\\n")'], on_output=handle)
    assert _join_out(res.output) == b'abc' + proc.OS_PREFERRED_NEWLINE
    assert acc == b'abc' + proc.OS_PREFERRED_NEWLINE

    acc = b''

    res = await proc.run([sys.executable, '-c', 'import sys; sys.stdout.write("abc")'], on_output=handle)
    assert _join_out(res.output) == b'abc'
    assert acc == b'abc'
