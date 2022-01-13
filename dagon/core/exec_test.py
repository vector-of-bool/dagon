from __future__ import annotations

import asyncio
from asyncio import Event
from typing import Iterable

import pytest
from dagon.core.result import NodeResult, Success

from . import exec, ll_dag
from .ll_dag import LowLevelDAG, NodeState


class CalcLengths:
    def __init__(self) -> None:
        self.strings: dict[str, int] = {}
        self.banned_strings: set[str] = set()

    def save_length(self, s: str) -> str:
        if s in self.banned_strings:
            raise RuntimeError(f'Banned string: {s}')
        self.strings[s] = len(s)
        return s


def test_calc_lengths():
    asyncio.set_event_loop(asyncio.new_event_loop())
    calc = CalcLengths()
    graph = ll_dag.LowLevelDAG(nodes=['foo', 'bar', 'baz'], edges=[('bar', 'baz'), ('foo', 'baz')])
    exe = exec.SimpleExecutor(graph.copy(), calc.save_length)
    res = exe.run_some_until_complete()
    assert exe.has_pending_work
    assert not exe.has_running_work
    assert set(res.values()) == {
        NodeResult('foo', Success('foo')),
        NodeResult('bar', Success('bar')),
    }
    assert calc.strings['foo'] == 3
    assert calc.strings['bar'] == 3
    res = exe.run_some_until_complete()
    assert set(res.values()) == {NodeResult('baz', Success('baz'))}
    assert not exe.has_pending_work
    assert not exe.has_running_work
    assert not exe.any_failed

    # Ban a string and assert that it has failed
    calc = CalcLengths()
    calc.banned_strings.add('foo')
    dup = graph.copy()
    exe = exec.SimpleExecutor(dup, calc.save_length)
    exe.run_some_until_complete()
    assert exe.has_pending_work
    assert not exe.has_running_work
    assert 'foo' not in calc.strings
    assert calc.strings['bar'] == 3
    assert exe.any_failed
    assert exe.finished


class EventWaiter:
    def __init__(self, names: Iterable[str]):
        self.events = {k: Event() for k in names}
        self.triggered_events: set[str] = set()

    async def wait_for(self, ev: str) -> str:
        await self.events[ev].wait()
        self.triggered_events.add(ev)
        return ev


@pytest.mark.asyncio
async def test_exec():
    lens = CalcLengths()
    graph = LowLevelDAG[str](nodes=['foo', 'bar', 'baz'], edges=[('foo', 'bar'), ('bar', 'baz')])
    exe = exec.SimpleExecutor(graph, lens.save_length)
    assert exe.has_pending_work
    assert graph.state_of('foo') == NodeState.Pending
    assert graph.state_of('bar') == NodeState.Pending
    assert graph.state_of('baz') == NodeState.Pending
    await exe.run_some()
    assert graph.state_of('foo') == NodeState.Finished
    assert graph.state_of('bar') == NodeState.Pending
    assert 'bar' in graph.ready_nodes
    assert exe.has_pending_work


@pytest.mark.asyncio
async def test_exec_interrupt():
    waiter = EventWaiter(['foo', 'bar', 'baz'])
    graph = LowLevelDAG[str](nodes=['foo', 'bar', 'baz'], edges=[('foo', 'bar'), ('foo', 'baz')])
    interruptor = Event()
    exe = exec.SimpleExecutor(graph, waiter.wait_for)
    assert exe.has_pending_work

    assert graph.state_of('foo') == NodeState.Pending
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(exe.run_some(interrupt=interruptor), 0.01)

    assert graph.state_of('foo') == NodeState.Running
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(exe.run_some(interrupt=interruptor), 0.01)

    interruptor.set()
    await asyncio.wait_for(exe.run_some(interrupt=interruptor), 1)
    assert graph.state_of('foo') == NodeState.Running

    interruptor.clear()

    # Trigger the 'foo' task to return
    waiter.events['foo'].set()
    res = await asyncio.wait_for(exe.run_some(interrupt=interruptor), 1)
    assert graph.state_of('foo') == NodeState.Finished
    assert set(res.values()) == {NodeResult('foo', Success('foo'))}
    assert exe.has_pending_work
    assert not exe.has_running_work
    assert graph.state_of('bar') == NodeState.Pending
    assert graph.state_of('baz') == NodeState.Pending

    assert 'bar' in graph.ready_nodes
    waiter.events['bar'].set()
    waiter.events['baz'].set()
    res = await asyncio.wait_for(exe.run_some(interrupt=interruptor), 1)
    assert not exe.has_pending_work
    assert not exe.has_running_work
    assert set(res.values()) == {
        NodeResult('bar', Success('bar')),
        NodeResult('baz', Success('baz')),
    }
