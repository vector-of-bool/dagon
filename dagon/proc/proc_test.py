import asyncio
import datetime
from ..event.cancel import CancellationToken
import sys

import pytest

from . import proc


@pytest.mark.asyncio
async def test_run_process_stdout():
    result = await proc.run([sys.executable, '-c', 'print("Hello!", end="")'])
    assert result.stdout == b'Hello!'


@pytest.mark.asyncio
async def test_run_proc_fail():
    result = await proc.run([sys.executable, '-c', 'import sys; sys.exit(42)'], check=False)
    assert result.retcode == 42
    assert result.stdout == b''
    assert result.stderr == b''


@pytest.mark.asyncio
async def test_run_cancel_via_token():
    token = CancellationToken()
    pipe = await proc.spawn([sys.executable, '-c', 'import time; time.sleep(100)'], cancel=token)
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
    assert result.stdout == b'\n'


@pytest.mark.asyncio
async def test_run_simple_stdin():
    result = await proc.run(
        [sys.executable, '-c', 'import sys; a = input("a:"); b = input("b:"); print("got:", a, b, sys.stdin.read())'],
        stdin='eggs\nbacon\n',
        timeout=datetime.timedelta(seconds=1),
        check=False)
    assert result.retcode == 0
    assert result.stdout == b'a:b:got: eggs bacon \n'


@pytest.mark.asyncio
async def test_output_handler():
    acc = ''

    def handle(data: bytes):
        nonlocal acc
        acc += data.decode()

    res = await proc.run([sys.executable, '-c', 'import sys; sys.stdout.write("abc\\n")'], on_line=handle)
    assert res.stdout == b'abc\n'
    assert acc == 'abc\n'

    acc = ''

    res = await proc.run([sys.executable, '-c', 'import sys; sys.stdout.write("abc")'], on_line=handle)
    assert res.stdout == b'abc'
    assert acc == 'abc'
