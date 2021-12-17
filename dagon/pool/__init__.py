"""
Module ``dagon.pool``
#####################

Task job pools for concurrency limits
"""
from __future__ import annotations

from asyncio import Semaphore
from contextlib import asynccontextmanager, nullcontext
from typing import (Any, AsyncContextManager, AsyncIterator, ContextManager, Hashable, NamedTuple)

from dagon.ext.iface import OpaqueTaskGraphView
from dagon.task.dag import OpaqueTask
from dagon.util import AsyncNullContext, nearest_matching

from ..ext.base import BaseExtension
from ..task.task import Task


class _PoolInfo(NamedTuple):
    name: str
    size: int


class _PoolsAppCtx(NamedTuple):
    pools: dict[str, _PoolInfo]
    assignments: dict[Hashable, str]


class _PoolsDagCtx(NamedTuple):
    pools: dict[str, Semaphore]
    assignments: dict[Hashable, Semaphore]


class _PoolsExt(BaseExtension[_PoolsAppCtx, _PoolsDagCtx, None]):
    dagon_ext_name = 'dagon.pools'

    def app_context(self) -> ContextManager[_PoolsAppCtx]:
        return nullcontext(_PoolsAppCtx({}, {}))

    def global_context(self, graph: OpaqueTaskGraphView) -> AsyncContextManager[_PoolsDagCtx]:
        appdat = self.app_data()
        semaphores = {pool.name: Semaphore(pool.size) for pool in appdat.pools.values()}
        assignments = {task: semaphores[pool_name] for task, pool_name in appdat.assignments.items()}
        return AsyncNullContext(_PoolsDagCtx(semaphores, assignments))

    @asynccontextmanager
    async def task_context(self, task: OpaqueTask) -> AsyncIterator[None]:
        gdat = self.global_data()
        sem = gdat.assignments.get(task)
        if sem is None:
            yield
            return
        async with sem:
            yield


def add(name: str, size: int) -> None:
    """
    Create a job pool in the current dag with the given name and size.

    :param name: A new unique name for the job pool. No other pool may exist
        with the same name.
    :param size: A positive non-zero integer for the number of jobs in the pool.
    """
    adat = _PoolsExt.app_data()
    if name in adat.pools:
        raise NameError(f'A pool named "{name}" is already defined')
    if size < 1:
        raise ValueError(f'Pools must have a size >=1, but "{name}" given size {size}')
    adat.pools[name] = _PoolInfo(name, size)


def assign(task: Task[Any], pool: str) -> None:
    """
    Assign the given task to the job pool with the given name. The pool must
    have been previous created using :func:`.add`. A task may only be assigned
    to one pool at a time.
    """
    adat = _PoolsExt.app_data()
    if task in adat.assignments:
        raise RuntimeError(f'Task "{task.name}" is already assigned to pool "{adat.assignments[task]}"')
    if pool not in adat.pools:
        nearest = nearest_matching(pool, adat.pools.values(), lambda p: p.name)
        if nearest:
            raise NameError(f'No pool "{pool}" is defined. (Did you mean "{nearest.name}"?)')
        raise NameError(f'No pool "{pool}" is defined')
    adat.assignments[task] = pool
