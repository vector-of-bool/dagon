from __future__ import annotations

import argparse
import contextvars
import sys
import warnings
from contextlib import (AsyncExitStack, ExitStack, asynccontextmanager, contextmanager)
from typing import (Any, AsyncContextManager, AsyncIterator, Callable, Iterable, Iterator, NamedTuple, Sequence, cast,
                    overload)

from dagon.core import ll_dag
from dagon.core.result import NodeResult
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
    """
    Obtain the application-level context data for the given extension.

    .. note:: The extension must be loaded and we must be within an application context.
    """
    return _get_ext_data(_CTX_APP_DATA, ext)


@overload
def ext_global_data(ext: IExtension[Any, GlobalDataT, Any]) -> GlobalDataT:
    ...


@overload
def ext_global_data(ext: str) -> Any:
    ...


def ext_global_data(ext: str | IExtension[Any, Any, Any]) -> Any:
    """
    Obtain the DAG-global context data for the given extension.

    .. note:: The extension must be loaded and we must be within a DAG exection context.
    """
    return _get_ext_data(_CTX_GLOBAL_DATA, ext)


@overload
def ext_task_data(ext: IExtension[Any, Any, TaskDataT]) -> TaskDataT:
    ...


@overload
def ext_task_data(ext: str) -> Any:
    ...


def ext_task_data(ext: str | IExtension[Any, Any, Any]) -> Any:
    """
    Obtain the task-level context data for the given extension.

    .. note:: The extension must be loaded and we must be within a task execution context.
    """
    return _get_ext_data(_CTX_TASK_DATA, ext)


_OpaqueExt = IExtension[Opaque, Opaque, Opaque]
"An opaque-typed extension"


class ExtLoader:
    """
    An extension loader and manager.

    All extensions should be loaded into this class, and this class can be used
    to control them as a group in the appropriate order.
    """
    def __init__(self) -> None:
        self._loaded: dict[str, _OpaqueExt] = {}
        "Active extensions by name"

    def load(self, ext: IExtension[Any, Any, Any]) -> None:
        """
        Load the given extension into the loader context. There must not already
        be an extension loaded by the same name.

        If the given extension has and extension dependencies, those will need
        to be loaded too before the loader can be used, but do not necessarily
        need to be loaded in order.
        """
        if ext.dagon_ext_name in self._loaded:
            raise NameError(f'Extension "{ext.dagon_ext_name}" is already loaded')
        self._loaded[ext.dagon_ext_name] = ext

    def _iter_order(self) -> Sequence[_OpaqueExt]:
        """Return a sequence of all extensions in appropriate dependency order"""
        return tuple(self._iter_order_1())

    def _iter_order_1(self) -> Iterable[_OpaqueExt]:
        visited: set[str] = set()
        for e in self._loaded.values():
            yield from self._iter_order_2(e, visited)

    def _iter_order_2(self, ext: _OpaqueExt, visited: set[str]) -> Iterable[_OpaqueExt]:
        # First load all of the requirements:
        for r in ext.dagon_ext_requires:
            # Skip if we have already loaded it
            if r in visited:
                continue
            if r not in self._loaded:
                # Raise an error if it has not been registered
                raise RuntimeError(f'No extension "{r}" is registered, but is required by "{ext.dagon_ext_name}"')
            yield from self._iter_order_2(self._loaded[r], visited)
        # And optional requirements:
        for r in ext.dagon_ext_requires_opt:
            if r in visited:
                continue
            if r in self._loaded:
                yield from self._iter_order_2(self._loaded[r], visited)
        # Now yield the actual one we are visiting:
        if ext.dagon_ext_name not in visited:
            yield ext
            visited.add(ext.dagon_ext_name)

    @asynccontextmanager
    async def _enter_context(
        self,
        cvar: contextvars.ContextVar[_ExtDataContext],
        proj: Callable[[_OpaqueExt], AsyncContextManager[Opaque]],
    ) -> AsyncIterator[None]:
        """
        Enter the async context, and store the context value in the given contextvar.

        Each extension will be entered using the ``proj`` projection function,
        which should map an extension to an associated async context manager.
        """
        async with AsyncExitStack() as st:
            data = _ExtDataContext({})
            st.enter_context(scope_set_contextvar(cvar, data))
            for p in self._iter_order():
                dat = await st.enter_async_context(proj(p))
                data.by_name[p.dagon_ext_name] = dat
            yield

    @contextmanager
    def app_context(self) -> Iterator[None]:
        """
        Enter the application-level context for all extensions. This is a non-async
        context.
        """
        with ExitStack() as st:
            data = _ExtDataContext({})
            st.enter_context(scope_set_contextvar(_CTX_APP_DATA, data))
            for p in self._iter_order():
                dat = st.enter_context(p.app_context())
                data.by_name[p.dagon_ext_name] = dat
            yield

    def global_context(self, graph: ll_dag.DAGView[OpaqueTask]) -> AsyncContextManager[None]:
        """Enter the DAG-global context for the given task graph"""
        return self._enter_context(_CTX_GLOBAL_DATA, lambda p: p.global_context(graph))

    def task_context(self, task: OpaqueTask) -> AsyncContextManager[None]:
        """Enter a task-local context for the given task"""
        return self._enter_context(_CTX_TASK_DATA, lambda p: p.task_context(task))

    async def notify_result(self, result: NodeResult[OpaqueTask]) -> None:
        """Notify all extensions of the result of a task"""
        for p in self._iter_order():
            await p.notify_result(result)

    def add_options(self, parser: argparse.ArgumentParser) -> None:
        """
        Allow all extensions to augment the command-line argument parser.
        """
        for p in self._loaded.values():
            p.add_options(parser)

    def handle_options(self, args: argparse.Namespace) -> None:
        """
        Allow all extensions to handle the result of command-line argument
        parsing.
        """
        for p in self._loaded.values():
            p.handle_options(args)

    @classmethod
    def default(cls) -> ExtLoader:
        """
        Obtain the default extension loaded loaded with the extensions
        defined by the "dagon.extensions" entrypoint. This will instantiate and
        load all extensions. Extensions that fail to load will be discarded with
        a warning message.
        """
        ret = ExtLoader()
        eps = cast(Iterable[EntryPoint], entry_points(group='dagon.extensions'))
        for ep in eps:
            cls._try_load_one(ret, ep)
        # Call the order function. This will raise an exception if any extensions
        # are loaded without their requirements having been loaded first.
        ret._iter_order()
        return ret

    @staticmethod
    def _try_load_one(ldr: ExtLoader, ep: EntryPoint) -> None:
        "Attempt to load one extension from the given EntryPoint"
        ep_name: str = getattr(ep, 'name', '<unnamed>')
        try:
            cls: _OpaqueExt | object = ep.load()
        except BaseException as e:
            warnings.warn(f"An exception occurred while trying to load extension {ep!r}: {e}",
                          ExtensionLoadWarning,
                          source=e)
            return
        # Be very careful now...
        # ext_name is required:
        if not hasattr(cls, 'dagon_ext_name'):
            warnings.warn(f'Extension object {cls!r} does not have a "dagon_ext_name" attribute', ExtensionLoadWarning)
            return
        ext_name = getattr(cls, 'dagon_ext_name')
        if not isinstance(ext_name, str):
            warnings.warn(f'Extension object {cls!r} has a non-string "dagon_ext_name": {ext_name!r}',
                          ExtensionLoadWarning)
            return
        # dagon_ext_name must match the entrypoint name:
        if ext_name != ep_name:
            warnings.warn(
                f'Extension object {cls!r} has a mismatching extension name. Expected "{ep_name}", but got "{ext_name}"',
                ExtensionLoadWarning)
            return
        # The extension must be a callable object:
        if not callable(cls):
            warnings.warn(f'Extension entrypoint {cls!r} is not callable', ExtensionLoadWarning)
            return
        # Attempt to create an instance now:
        try:
            inst: Any = cls()
        except BaseException as e:
            warnings.warn(f'Instantiating/calling extension loader [{cls!r}] resulted in an exception: {e}',
                          ExtensionLoadWarning,
                          source=e)
            return
        # Check that the extension implements the interface using a type-checkable Protocol:
        if not isinstance(inst, IExtension):
            warnings.warn(f'Generated extension object {inst!r} does not implement the necessary interface')
            return
        # Got it:
        ldr.load(cast(_OpaqueExt, inst))
