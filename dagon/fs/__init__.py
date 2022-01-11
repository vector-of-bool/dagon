"""
Module ``dagon.fs``
###################

Module for performing filesystem operations
"""

from __future__ import annotations

import asyncio
import errno
import functools
import itertools
import os
import shutil
from concurrent.futures import Executor, ThreadPoolExecutor
from io import BufferedIOBase
from pathlib import Path, PurePath
from typing import (TYPE_CHECKING, Any, AsyncContextManager, AsyncIterable, AsyncIterator, Awaitable, Callable, Generic,
                    Iterable, Mapping, Type, TypeVar, Union, cast)

from typing_extensions import Literal, ParamSpec

from ..event.cancel import CancellationToken, raise_if_cancelled
from ..util import T
from ..util.doc import __sphinx_build__

Pathish = Union['os.PathLike[str]', str]
'A path-able argument. A string, subclass of `~pathlib.PurePath`, or anything with an ``__fspath__`` member'
NPaths = Union[Pathish, Iterable[Pathish]]
'Convenience type to represent either a single path or an iterable list of paths'
FilePredicate = Callable[[Path], bool]
'Type for predicate functions that accept a file path and return a `bool`'

_FS_OPS_POOL = ThreadPoolExecutor(4)

_PathT = TypeVar('_PathT', bound=Pathish)

_Params = ParamSpec('_Params')

if TYPE_CHECKING:

    class _ExecutorMappedOperation(Generic[_Params, T]):
        def __init__(self, sync_func: Callable[_Params, T], executor: Executor) -> None:
            ...

        def sync(self, *args: _Params.args, **kwargs: _Params.kwargs) -> T:
            ...

        def __call__(self, *args: _Params.args, **kwargs: _Params.kwargs) -> Awaitable[T]:
            ...

else:
    _FuncT = TypeVar('_FuncT', bound=Callable[..., Any])

    class _ExecutorMappedOperation(Generic[_FuncT, T]):
        def __init__(self, sync_func: _FuncT, executor: Executor) -> None:
            self.__name__ = sync_func.__name__
            self.__doc__ = sync_func.__doc__
            self._sync = sync_func
            self._exec = executor

        def sync(self, *args: Any, **kwargs: Any) -> T:
            return self._sync(*args, **kwargs)

        def __call__(self, *args: Any, **kwargs: Any) -> Awaitable[T]:
            loop = asyncio.get_running_loop()
            cancel = CancellationToken.get_context_local()
            return loop.run_in_executor(self._exec, lambda: self.sync(*args, **kwargs, cancel=cancel))


def _thread_pooled(fn: Callable[_Params, T]) -> _ExecutorMappedOperation[_Params, T]:
    if __sphinx_build__:
        return cast(Any, fn)
    r = _ExecutorMappedOperation[_Params, T](fn, _FS_OPS_POOL)
    functools.update_wrapper(r, fn)
    return r


def iter_pathish(p: NPaths, *, type: Type[_PathT] = Path) -> Iterable[_PathT]:
    """
    Given an iterable of paths or a single path, yield either the single path
    or each of the paths given.
    """
    if isinstance(p, (str, PurePath)) or hasattr(p, '__fspath__'):
        yield type(p)  # type: ignore
    else:
        yield from (type(i) for i in p)  # type: ignore


IfExists = Literal['replace', 'fail', 'keep']
"""Policy for performing file operations where the destination already exists"""

IfDirectoryExists = Literal['replace', 'fail', 'keep', 'merge']
"""Policy for performing file operations where the destination directory already exists"""


def fs_thread_pool() -> ThreadPoolExecutor:
    """
    The thread pool that is used to execute filesystem operations
    """
    return _FS_OPS_POOL


def _run_fs_op(fn: Callable[[], None]) -> 'asyncio.Future[None]':
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(fs_thread_pool(), fn)  # type: ignore


def _copy_file_no_overwrite(src: Path, dst: Path) -> None:
    with src.open('rb') as f:
        with dst.open('xb') as out:
            shutil.copyfileobj(f, out)


@_thread_pooled
def copy_file(file: Pathish,
              dest: Pathish,
              *,
              if_exists: IfExists = 'fail',
              mkdirs: bool = True,
              preserve_symlinks: bool = True,
              preserve_stat: bool = False,
              cancel: CancellationToken | None = None) -> None:
    """
    Copy a file from one location to another.

    :param file: Path to a file to copy.
    :param dest: The full filepath for the destination.
    :param on_exists: What to do if the destination already exists.
    :param mkdirs: If ``True``, make intermediate directories for the destination.
    :param preserve_symlinks: If ``True``, and ``file`` refers to a symlink, the
        symlink will be copied (rather than the target thereof)
    :param preserve_stat: If ``True``, attributes of the source file will be
        applied to the destination.
    :param cancel: A cancellation token to cancel the operation.

    .. note::
        ``dest`` is the full path to a file, not the path to the directory that
        should receive the file.
    """
    raise_if_cancelled(cancel)
    file = Path(file)
    dest = Path(dest)
    pardir = dest.parent
    if not pardir.exists() and mkdirs:
        pardir.mkdir(exist_ok=True, parents=True)

    do_copy: Callable[[Path, Path], Any]
    if preserve_symlinks and file.is_symlink():
        # Copy the link, not the target of the link
        do_copy = lambda s, d: os.symlink(os.readlink(s), d)
    else:
        # Copy file, but raise if we attempt to overwrite it
        do_copy = _copy_file_no_overwrite
    try:
        do_copy(file, dest)
    except FileExistsError:
        if if_exists == 'fail':
            raise
        if if_exists == 'keep':
            return
        assert if_exists == 'replace'
        # Delete the file, and do it again.
        dest.unlink()
        do_copy(file, dest)

    if preserve_stat:
        shutil.copystat(file, dest, follow_symlinks=False)
    else:
        shutil.copymode(file, dest, follow_symlinks=False)


@_thread_pooled
def copy_tree(inpath: Pathish,
              destpath: Pathish,
              *,
              mkdirs: bool = True,
              if_exists: IfDirectoryExists = 'fail',
              if_file_exists: IfExists = 'fail',
              only_if: FilePredicate | None = None,
              unless: FilePredicate | None = None,
              preserve_symlinks: bool = True,
              preserve_stat: bool = False,
              cancel: CancellationToken | None = None) -> None:
    """
    Copy a tree file files/directories.

    :param inpath: Path to a file/directory to copy.
    :param destpath: The destination path to copy to.
    :param mkdirs: If ``True``, create intermediate directories.
    :param on_exists: The action to take if the destination directory exists.
    :param on_file_exists: The action to take if any files within the destination
        directory already exists. (This is applicable to ``"merge"``)
    :param only_if: A predicate function that takes an absolute ``Path`` and
        returns a ``bool``. If provided, only files for which this predicate
        returns ``True`` will be copied into the destination.
    :param unless: A predictae function that takes an absolute ``Path`` and
        returns a ``bool``. If provided, only files for which this predicate
        returns ``False`` will be copied into the destination.
    :param preserve_symlinks: Whether to copy symlinks as symlinks, or follow
        them through to copy their targets.
    :param preserve_stat: If ``True``, attributes from the source will be
        applied to the destination.
    :param cancel: A cancellation token for the operation.

    .. note::
        If both ``only_if`` and ``unless`` are provided, ``only_if`` will be
        checked *first*, and then ``unless`` will be checked. Both predicates
        will be considered.
    """
    raise_if_cancelled(cancel)
    indir = Path(inpath).absolute()
    destdir = Path(destpath)
    pardir = destdir.parent
    if not pardir.is_dir():
        if mkdirs:
            pardir.mkdir(exist_ok=True, parents=True)
        else:
            raise RuntimeError(f'Cannot copy directory {indir} to {destdir}: Path {pardir} '
                               'is not an existing directory')
    if destdir.exists():
        if if_exists == 'replace':
            shutil.rmtree(destdir)
        if if_exists == 'fail':
            raise RuntimeError(f'Cannot copy {indir} to {destdir}: Destination already exists')
        if if_exists == 'keep':
            return
        if if_exists == "merge":
            pass

    destdir.mkdir(exist_ok=True, parents=True)
    raise_if_cancelled(cancel)
    for in_abs in indir.iterdir():
        raise_if_cancelled(cancel)
        dest_abs = destdir / (in_abs.relative_to(indir))
        # Item is a symlink, and that symlink refers to a directory, and
        # we are not being asked to preserve symlinks. Resolve the link
        # and copy the tree from the directory:
        should_follow_dir_symlink = (in_abs.is_symlink() and not preserve_symlinks and in_abs.resolve().is_dir())
        # Whether we should copy the tree:
        should_copy_tree = in_abs.is_dir() or should_follow_dir_symlink
        if should_copy_tree:
            copy_tree.sync(in_abs,
                           dest_abs,
                           if_file_exists=if_file_exists,
                           only_if=only_if,
                           unless=unless,
                           preserve_stat=preserve_stat,
                           preserve_symlinks=preserve_symlinks,
                           cancel=cancel)
            # Process the next item
            continue

        # This is a file, not a directory
        if only_if is not None and not only_if(in_abs):
            # The file has been excluded by the `only_if` filter
            continue
        if unless is not None and unless(in_abs):
            # The file has been excluded by the `unless` filter
            continue
        copy_file.sync(in_abs,
                       dest_abs,
                       if_exists=if_file_exists,
                       mkdirs=False,
                       preserve_symlinks=preserve_symlinks,
                       preserve_stat=preserve_stat)


TreeItem = Union[bytes, Mapping[str, 'TreeItem']]


async def create_tree(root: Pathish, items: Mapping[str, TreeItem], *, cancel: CancellationToken | None = None) -> None:
    raise_if_cancelled(cancel)
    return await _run_fs_op(lambda: _create_fs_tree(root, items, cancel))


def _create_fs_tree(root: Pathish, items: Mapping[str, TreeItem], cancel: CancellationToken | None) -> None:
    root = Path(root)
    root.mkdir(exist_ok=True, parents=True)
    for filename, item in items.items():
        raise_if_cancelled(cancel)
        if isinstance(item, bytes):
            root.joinpath(filename).write_bytes(item)
        else:
            _create_fs_tree(root / filename, item, cancel)


def _remove(files: NPaths, recurse: bool, absent_ok: bool) -> None:
    for filepath in iter_pathish(files):
        try:
            if filepath.is_dir() and recurse:
                shutil.rmtree(filepath)
            else:
                os.unlink(filepath)
        except FileNotFoundError:
            if not absent_ok:
                raise


@_thread_pooled
def remove(files: NPaths,
           *,
           recurse: bool = False,
           absent_ok: bool = False,
           cancel: CancellationToken | None = None) -> None:
    """
    Remove a file or directory.

    :param files: Path(s) to an existing files/directories to remove.
    :param recurse: If ``True`` and ``filepath`` names a directory, recursively
        remove the directory as if by :func:`shutil.rmtree`.
    :param absent_ok: If ``True`` and the filepath does not exist, no error will
        be raised. This can create simple idempotent file/directory removal.
    :param cancel: A cancellation token for the operation.
    """
    for filepath in iter_pathish(files):
        raise_if_cancelled(cancel)
        try:
            if filepath.is_dir() and recurse:
                shutil.rmtree(filepath)
            else:
                os.unlink(filepath)
        except FileNotFoundError:
            if not absent_ok:
                raise


@_thread_pooled
def clear_directory(dirs: NPaths, *, cancel: CancellationToken | None = None) -> None:
    """
    Remove the contents of a directory, but keep the directory.

    :param dirs: Path(s) to an directories to clear.
    :param cancel: A cancellation token for the operation.
    """
    raise_if_cancelled(cancel)
    each_dir = iter_pathish(dirs)
    children_per_dir = (d.iterdir() for d in each_dir)
    all_children = itertools.chain.from_iterable(children_per_dir)
    _remove(all_children, recurse=True, absent_ok=True)


def _read_some(fd: BufferedIOBase, buf: bytearray) -> int:
    return fd.readinto(buf)


async def read_blocks_from(io: BufferedIOBase,
                           *,
                           block_size: int = 1024 * 10,
                           cancel: CancellationToken | None = None) -> AsyncIterator[memoryview]:
    buf = bytearray(block_size)
    loop = asyncio.get_event_loop()
    while True:
        raise_if_cancelled(cancel)
        nread = await loop.run_in_executor(_FS_OPS_POOL, lambda: _read_some(io, buf))
        if nread == 0:
            break
        yield memoryview(buf)[:nread]


async def read_blocks(filepath: Pathish,
                      *,
                      block_size: int = 1024 * 10,
                      cancel: CancellationToken | None = None) -> AsyncIterable[memoryview]:
    """
    Asynchronously lazily ready blocks from the given file.
    """
    raise_if_cancelled(cancel)
    filepath = Path(filepath)
    with filepath.open('rb') as fd:
        async for b in read_blocks_from(fd, block_size=block_size, cancel=cancel):
            yield b


@_thread_pooled
def safe_move_file(source: Pathish,
                   dest: Pathish,
                   *,
                   mkdirs: bool = True,
                   if_exists: IfExists = 'fail',
                   cancel: CancellationToken | None = None) -> Path:
    """
    Move the given file from one location to another. If the system cannot
    simply move the file, it will be copied and the source will be removed.

    :param source: The source file which will be moved.
    :param dest: The destination of the move operation.
    :param mkdirs: If ``True``, the directory containing ``dest`` will be created if absent.
    :param on_exists: What to do if the destination already exists.
    :param cancel: A cancellation token for the operation.
    """
    source = Path(source)
    dest = Path(dest)
    raise_if_cancelled(cancel)
    if mkdirs:
        dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        source.rename(dest)
        return dest
    except OSError as e:
        # Check if the destination file already exists
        if e.errno == errno.EEXIST:
            if if_exists == 'fail':
                # User wants to receive this exception
                raise
            if if_exists == 'keep':
                # User wants to keep the destination file
                return dest
            # The alternative is to replace
            assert if_exists == 'replace', if_exists
            # User wants us to replace the file in the destination
            dest.unlink()
            return safe_move_file.sync(source, dest, mkdirs=mkdirs, if_exists=if_exists, cancel=cancel)
        if e.errno == errno.EXDEV:
            # Cross-device linking: We're trying to relink a file across a filesystem boundary. This is where the "safe" comes in.
            copy_file.sync(source, dest, if_exists=if_exists, mkdirs=True, preserve_symlinks=False, preserve_stat=True)
            source.unlink()
            return dest
        raise


def open_file_writer(fpath: Pathish, if_exists: IfExists = 'fail'):
    """
    Opens a native file as an `~IFileWriter`.
    """
    from ..storage import NativeFileStorage
    return NativeFileStorage('/').open_file_writer(fpath, if_exists=if_exists)


def read_file(fpath: Pathish) -> AsyncContextManager[AsyncIterable[bytes]]:
    """
    Opens a native file as an asyncronous byte iterable.
    """
    from ..storage import NativeFileStorage
    return NativeFileStorage('/').read_file(fpath)
