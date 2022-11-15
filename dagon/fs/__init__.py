"""
Module ``dagon.fs``
###################

Module for performing filesystem operations
"""

from __future__ import annotations

import asyncio
import errno
import itertools
import os
import shutil
import stat
from concurrent.futures import ThreadPoolExecutor
from io import BufferedIOBase
from pathlib import Path, PurePath
from types import TracebackType
from typing import (AsyncContextManager, AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, Mapping, Type,
                    TypeVar, Union, overload)

from typing_extensions import Literal

from .. import util
from ..event import CancellationToken, raise_if_cancelled
from ..util import T

Pathish = Union['os.PathLike[str]', str]
'A path-able argument. A string, subclass of `~pathlib.PurePath`, or anything with an ``__fspath__`` member'
NPaths = Union[Pathish, Iterable[Pathish]]
'Convenience type to represent either a single path or an iterable list of paths'
FilePredicate = Callable[[Path], bool]
'Type for predicate functions that accept a file path and return a `bool`'

_FS_OPS_POOL = ThreadPoolExecutor(4)

_PathT = TypeVar('_PathT', bound=Pathish)


@overload
def iter_pathish(p: NPaths) -> Iterable[Path]:
    ...


@overload
def iter_pathish(p: NPaths, *, fac: Callable[[Pathish], _PathT]) -> Iterable[_PathT]:
    ...


def iter_pathish(p: NPaths, *, fac: Callable[[Pathish], _PathT] = Path) -> Iterable[_PathT]:
    """
    Given an iterable of paths or a single path, yield either the single path
    or each of the paths given.
    """
    if isinstance(p, (str, PurePath)) or hasattr(p, '__fspath__'):
        yield fac(p)  # type: ignore
    else:
        yield from (fac(i) for i in p)  # type: ignore


IfExists = Literal['replace', 'fail', 'keep']
"""Policy for performing file operations where the destination already exists"""

IfDirectoryExists = Literal['replace', 'fail', 'keep', 'merge']
"""Policy for performing file operations where the destination directory already exists"""


def fs_thread_pool() -> ThreadPoolExecutor:
    """
    The thread pool that is used to execute filesystem operations
    """
    return _FS_OPS_POOL


def _run_fs_op(fn: Callable[[], T]) -> asyncio.Future[T]:
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(fs_thread_pool(), fn)  # type: ignore


def _copy_file_no_overwrite(src: Path, dst: Path) -> None:
    with src.open('rb') as f:
        with dst.open('xb') as out:
            shutil.copyfileobj(f, out)


def _any_parent_is_file(p: Path) -> bool:
    "Determine whether 'p' (or any ancestor of 'p') names a file"
    if p.is_dir():
        return False
    if p.is_file():
        return True
    return _any_parent_is_file(p.parent)


async def copy_file(file: Pathish,
                    dest: Pathish,
                    *,
                    if_exists: IfExists = 'fail',
                    mkdirs: bool = True,
                    preserve_symlinks: bool = True,
                    preserve_stat: bool = False,
                    cancel: CancellationToken | None = None) -> Path:
    """
    Copy a file from one location to another.

    :param file: Path to a file to copy.
    :param dest: The full filepath for the file destination.
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
    cancel = cancel or CancellationToken.get_context_local()
    raise_if_cancelled(cancel)
    file = Path(file)
    dest = Path(dest)
    pardir = dest.parent
    if not pardir.exists() and mkdirs:
        pardir.mkdir(exist_ok=True, parents=True)

    stat = file.stat(follow_symlinks=False)
    is_small = stat.st_size < 1024 * 64
    do_copy: Callable[[Path, Path], None]
    if preserve_symlinks and file.is_symlink():
        # Copy the link, not the target of the link
        do_copy = lambda s, d: os.symlink(os.readlink(s), d)
    else:
        # Copy file, but raise if we attempt to overwrite it
        do_copy = _copy_file_no_overwrite
    try:
        if is_small:
            do_copy(file, dest)
        else:
            await _run_fs_op(lambda: do_copy(file, dest))
    except FileNotFoundError as e:
        # Windows might not raise NotADirectoryError itself, so we'll check that
        # case ourselves
        if _any_parent_is_file(dest):
            raise NotADirectoryError from e
        raise
    except PermissionError as e:
        if file.is_dir():
            raise IsADirectoryError from e
        raise
    except FileExistsError:
        if if_exists == 'fail':
            raise
        if if_exists == 'keep':
            return dest
        assert if_exists == 'replace'
        # Delete the file, and do it again.
        dest.unlink()
        if is_small:
            do_copy(file, dest)
        else:
            await _run_fs_op(lambda: do_copy(file, dest))

    if preserve_stat:
        shutil.copystat(file, dest, follow_symlinks=False)
    else:
        shutil.copymode(file, dest, follow_symlinks=False)
    return dest


async def copy_tree(inpath: Pathish,
                    destpath: Pathish,
                    *,
                    mkdirs: bool = True,
                    if_exists: IfDirectoryExists = 'fail',
                    if_file_exists: IfExists = 'fail',
                    only_if: FilePredicate | None = None,
                    unless: FilePredicate | None = None,
                    preserve_symlinks: bool = True,
                    preserve_stat: bool = False,
                    cancel: CancellationToken | None = None) -> Path:
    """
    Copy a tree file files/directories.

    :param inpath: Path to a file/directory to copy.
    :param destpath: The destination path to copy to.
    :param mkdirs: If ``True``, create intermediate directories.
    :param on_exists: The action to take if the destination directory exists.
    :param on_file_exists: The action to take if any files within the destination
        directory already exists. (This is applicable to ``on_exists="merge"``)
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
    cancel = cancel or CancellationToken.get_context_local()
    raise_if_cancelled(cancel)
    indir = Path(inpath).absolute()
    destdir = Path(destpath)
    pardir = destdir.parent
    if not pardir.is_dir():
        if mkdirs:
            pardir.mkdir(exist_ok=True, parents=True)
        else:
            raise NotADirectoryError(f'Cannot copy directory {indir} to {destdir}: Path {pardir} '
                                     'is not an existing directory')
    if destdir.exists():
        if if_exists == 'replace':
            shutil.rmtree(destdir)
        if if_exists == 'fail':
            raise FileExistsError(f'Cannot copy {indir} to {destdir}: Destination already exists')
        if if_exists == 'keep':
            return destdir
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
            await copy_tree(in_abs,
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
        await copy_file(in_abs,
                        dest_abs,
                        if_exists=if_file_exists,
                        mkdirs=False,
                        preserve_symlinks=preserve_symlinks,
                        preserve_stat=preserve_stat)
    if preserve_stat:
        shutil.copystat(indir, destdir, follow_symlinks=False)
    return destdir


TreeItem = Union[bytes, Mapping[str, 'TreeItem']]


async def create_tree(root: Pathish, items: Mapping[str, TreeItem], *, cancel: CancellationToken | None = None) -> None:
    cancel = cancel or CancellationToken.get_context_local()
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


def _unlink_file(fpath: Path) -> None:
    fpath.unlink()


_delete_increment = 0


def _remove1(f: Path, recurse: bool, absent_ok: bool, cancel: CancellationToken | None) -> Awaitable[None]:
    raise_if_cancelled(cancel)
    global _delete_increment
    _delete_increment += 1
    tmpname = f.parent / f'{f.name}.del-{_delete_increment}'
    try:
        f.rename(tmpname)
    except FileNotFoundError:
        if not absent_ok:
            raise
        # We're okay if this file is missing
        return util.ReadyAwaitable(None)
    try:
        _unlink_file(tmpname)
    except IsADirectoryError:
        if not recurse:
            # This is a directory, and we are not set to recursively delete things
            raise
        return _run_fs_op(lambda: _remove_dir(tmpname, cancel))
    return util.ReadyAwaitable(None)


def _remove_dir(dirpath: Path, cancel: CancellationToken | None) -> None:
    raise_if_cancelled(cancel)
    shutil.rmtree(dirpath, onerror=_rmtree_on_error)


def _rmtree_on_error(
    fn: Callable[[str], None],
    path: str,
    _exc: tuple[Type[BaseException], BaseException, TracebackType],
) -> None:
    try:
        raise
    except PermissionError as e:
        if e.errno != 5:
            raise
        os.chmod(path, stat.S_IWRITE)
        fn(path)


def remove(files: NPaths,
           *,
           recurse: bool = False,
           absent_ok: bool = False,
           cancel: CancellationToken | None = None) -> Awaitable[None]:
    """
    Remove one or more files or directories.

    :param files: Path(s) to files and/or directories to remove.
    :param recurse: If ``True`` and any of ``files`` names a directory,
        recursively remove the directory and all of its contents.
    :param absent_ok: If ``True`` and any named file does not exist, no error
        will be raised. This can create simple idempotent file/directory
        removal.
    :param cancel: A cancellation token for the operation.

    .. hint::

        This "remove" function is optimized for getting files "out of the way"
        as fast as possible. As soon as this function returns (even before the
        return value is ``await``\\ ed), all named files/directories will be
        gone from their original location.

        Before being deleted from disk, the named files/directories are moved to
        a new temporary name. This move operation is extremely fast, whereas a
        recursive directory deletion might be extremely slow. For this reason,
        you can call :func:`remove` without awaiting, then act "as if" the files
        are already deleted (they are moved to a temporary location and being
        deleted in the background). Await the result of :func:`remove` to block
        until the deletion operations are actually completed.
    """
    cancel = cancel or CancellationToken.get_context_local()
    if isinstance(files, (str, PurePath)):
        return _remove1(Path(files), recurse, absent_ok, cancel)
    multi_files = iter_pathish(files)
    multi_rm = map(lambda f: _remove1(f, recurse, absent_ok, cancel), multi_files)
    futs = set(map(asyncio.ensure_future, multi_rm))
    return _remove_gather(futs)


async def _remove_gather(futs: set[asyncio.Task[None]]) -> None:
    done, pending = await asyncio.wait(futs, return_when=asyncio.FIRST_EXCEPTION)
    for t in pending:
        # 'pending' is non-empty iff an exception occurred. Cancel all other
        # running operations.
        t.cancel()
    for f in done:
        await f
    assert len(done) == len(futs)


def clear_directory(dirs: NPaths, *, cancel: CancellationToken | None = None) -> Awaitable[None]:
    """
    Remove the contents of one or more directories, but keep the directories.

    :param dirs: Path(s) to an directories to clear.
    :param cancel: A cancellation token for the operation.
    """
    each_dir = iter_pathish(dirs)
    children_per_dir = (d.iterdir() for d in each_dir)
    all_children = itertools.chain.from_iterable(children_per_dir)
    return remove(all_children, recurse=True, absent_ok=True, cancel=cancel)


def _read_some(fd: BufferedIOBase, buf: bytearray) -> int:
    return fd.readinto(buf)


async def read_blocks_from(io: BufferedIOBase,
                           *,
                           block_size: int = 1024 * 10,
                           cancel: CancellationToken | None = None) -> AsyncIterator[memoryview]:
    buf = bytearray(block_size)
    cancel = CancellationToken.get_context_local()
    while True:
        raise_if_cancelled(cancel)
        nread = await _run_fs_op(lambda: _read_some(io, buf))
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


def safe_move_file(source: Pathish,
                   dest: Pathish,
                   *,
                   mkdirs: bool = True,
                   if_exists: IfExists = 'fail',
                   cancel: CancellationToken | None = None) -> Awaitable[Path]:
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
    cancel = cancel or CancellationToken.get_context_local()
    raise_if_cancelled(cancel)
    if mkdirs:
        dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        source.rename(dest)
        return util.ReadyAwaitable(dest)
    except OSError as e:
        # Check if the destination file already exists
        if e.errno == errno.EEXIST:
            if if_exists == 'fail':
                # User wants to receive this exception
                raise
            if if_exists == 'keep':
                # User wants to keep the destination file
                return util.ReadyAwaitable(dest)
            # The alternative is to replace
            c: Literal['replace'] = if_exists
            util.unused(c)
            # User wants us to replace the file in the destination
            _unlink_file(dest)
            return safe_move_file(source, dest, mkdirs=mkdirs, if_exists=if_exists, cancel=cancel)
        if e.errno == errno.EXDEV:
            # Cross-device linking: We're trying to relink a file across a filesystem
            # boundary. This is where the "safe" comes in.
            return _teleport_file(source, dest, cancel)
        raise


async def _teleport_file(source: Path, dest: Path, cancel: CancellationToken | None) -> Path:
    await copy_file(file=source, dest=dest, cancel=cancel, preserve_stat=True, preserve_symlinks=True)
    _unlink_file(source)
    return dest


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
