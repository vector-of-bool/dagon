from __future__ import annotations

import argparse
import contextvars
import sys
import warnings
from contextlib import (AsyncExitStack, ExitStack, asynccontextmanager, contextmanager)
from typing import (Any, AsyncContextManager, AsyncIterator, Callable, Iterable, Iterator, NamedTuple, cast, overload)

from dagon.core import ll_dag
from dagon.task.dag import OpaqueTask
from dagon.util import Opaque, scope_set_contextvar

from .iface import AppDataT, GlobalDataT, IExtension, TaskDataT

if sys.version_info < (3, 10):
    from importlib_metadata import EntryPoint, entry_points
else:
    from importlib.metadata import EntryPoint, entry_points


class _ExtDataContext(NamedTuple):
    by_name: dict[str, Opaque]


class ExtensionLoadWarning(RuntimeWarning):
    pass


# The per-context per-extension extension data
_CTX_APP_DATA = contextvars.ContextVar[_ExtDataContext]('_CTX_APP_DATA')
_CTX_GLOBAL_DATA = contextvars.ContextVar[_ExtDataContext]('_CTX_GLOBAL_DATA')
_CTX_TASK_DATA = contextvars.ContextVar[_ExtDataContext]('_CTX_TASK_DATA')


def _get_ext_data(cvar: contextvars.ContextVar[_ExtDataContext], ext: str | IExtension[Any, Any, Any]) -> Any:
    if not isinstance(ext, str):
        ext = ext.dagon_ext_name
    ctx = cvar.get()
    try:
        return ctx.by_name[ext]
    except KeyError as e:
        raise RuntimeError(f'No extension "{ext}" is loaded in the requested context') from e


@overload
def ext_app_data(ext: IExtension[AppDataT, Any, Any]) -> AppDataT:
    ...


@overload
def ext_app_data(ext: str) -> Any:
    ...


def ext_app_data(ext: str | IExtension[Any, Any, Any]) -> Any:
    return _get_ext_data(_CTX_APP_DATA, ext)


@overload
def ext_global_data(ext: IExtension[Any, GlobalDataT, Any]) -> GlobalDataT:
    ...


@overload
def ext_global_data(ext: str) -> Any:
    ...


def ext_global_data(ext: str | IExtension[Any, Any, Any]) -> Any:
    return _get_ext_data(_CTX_GLOBAL_DATA, ext)


@overload
def ext_task_data(ext: IExtension[Any, Any, TaskDataT]) -> TaskDataT:
    ...


@overload
def ext_task_data(ext: str) -> Any:
    ...


def ext_task_data(ext: str | IExtension[Any, Any, Any]) -> Any:
    return _get_ext_data(_CTX_TASK_DATA, ext)


_OpaqueExt = IExtension[Opaque, Opaque, Opaque]


class ExtLoader:
    def __init__(self) -> None:
        self._loaded: dict[str, _OpaqueExt] = {}
        # Build the event map

    def load(self, ext: IExtension[Any, Any, Any]) -> None:
        if ext.dagon_ext_name in self._loaded:
            raise NameError(f'Extension "{ext.dagon_ext_name}" is already loaded')
        self._loaded[ext.dagon_ext_name] = ext

    def _iter_order(self) -> Iterable[_OpaqueExt]:
        s: set[str] = set()
        for p in self._loaded.values():
            yield from self._iter_order_1(p, s)

    def _iter_order_1(self, p: _OpaqueExt, visited: set[str]) -> Iterable[_OpaqueExt]:
        for r in p.dagon_ext_requires:
            if r in visited:
                continue
            if r not in self._loaded:
                raise RuntimeError(f'No extension "{r}" is registered, but is required by "{p.dagon_ext_name}"')
            yield from self._iter_order_1(self._loaded[r], visited)
        for r in p.dagon_ext_requires_opt:
            if r in visited:
                continue
            if r in self._loaded:
                yield from self._iter_order_1(self._loaded[r], visited)
        if p.dagon_ext_name not in visited:
            yield p
            visited.add(p.dagon_ext_name)

    @asynccontextmanager
    async def _enter_context(
        self,
        cvar: contextvars.ContextVar[_ExtDataContext],
        proj: Callable[[_OpaqueExt], AsyncContextManager[Opaque]],
    ) -> AsyncIterator[None]:
        async with AsyncExitStack() as st:
            data = _ExtDataContext({})
            st.enter_context(scope_set_contextvar(cvar, data))
            for p in self._iter_order():
                dat = await st.enter_async_context(proj(p))
                data.by_name[p.dagon_ext_name] = dat
            yield

    @contextmanager
    def app_context(self) -> Iterator[None]:
        with ExitStack() as st:
            data = _ExtDataContext({})
            st.enter_context(scope_set_contextvar(_CTX_APP_DATA, data))
            for p in self._iter_order():
                dat = st.enter_context(p.app_context())
                data.by_name[p.dagon_ext_name] = dat
            yield

    def global_context(self, graph: ll_dag.DAGView[OpaqueTask]) -> AsyncContextManager[None]:
        return self._enter_context(_CTX_GLOBAL_DATA, lambda p: p.global_context(graph))

    def task_context(self, task: OpaqueTask) -> AsyncContextManager[None]:
        return self._enter_context(_CTX_TASK_DATA, lambda p: p.task_context(task))

    def add_options(self, parser: argparse.ArgumentParser) -> None:
        for p in self._loaded.values():
            p.add_options(parser)

    def handle_options(self, args: argparse.Namespace) -> None:
        for p in self._loaded.values():
            p.handle_options(args)

    @classmethod
    def default(cls) -> ExtLoader:
        ret = ExtLoader()
        eps = cast(Iterable[EntryPoint], entry_points(group='dagon.extensions'))
        for ep in eps:
            cls._try_load_one(ret, ep)
        return ret

    @staticmethod
    def _try_load_one(ldr: ExtLoader, ep: EntryPoint) -> None:
        ep_name: str = getattr(ep, 'name', '<unnamed>')
        try:
            cls: _OpaqueExt | object = ep.load()
        except BaseException as e:
            warnings.warn(f"An exception occurred while trying to load extension {ep!r}: {e}",
                          ExtensionLoadWarning,
                          source=e)
            return
        if not hasattr(cls, 'dagon_ext_name'):
            warnings.warn(f'Extension object {cls!r} does not have a "dagon_ext_name" attribute', ExtensionLoadWarning)
            return
        ext_name = getattr(cls, 'dagon_ext_name')
        if not isinstance(ext_name, str):
            warnings.warn(f'Extension object {cls!r} has a non-string "dagon_ext_name": {ext_name!r}',
                          ExtensionLoadWarning)
            return
        if ext_name != ep_name:
            warnings.warn(
                f'Extension object {cls!r} has a mismatching extension name. Expected "{ep_name}", but got "{ext_name}"',
                ExtensionLoadWarning)
            return
        if not callable(cls):
            warnings.warn(f'Extension entrypoint {cls!r} is not callable', ExtensionLoadWarning)
            return
        try:
            inst: Any = cls()
        except BaseException as e:
            warnings.warn(f'Instaniating/calling extension loader [{cls!r}] resulted in an exception',
                          ExtensionLoadWarning,
                          source=e)
            return
        if not isinstance(inst, IExtension):
            warnings.warn(f'Generated extension object {inst!r} does not implement the necessary interface')
            return
        ldr.load(cast(_OpaqueExt, inst))
