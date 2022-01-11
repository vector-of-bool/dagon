"""
Utilities for creating/reading/modifying archives
"""

from __future__ import annotations

import asyncio
import enum
import functools
import io
import os
import tarfile
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePosixPath
from typing import (IO, Awaitable, Callable, Iterable, NamedTuple, Sequence, TypeVar, Union, cast)
from typing_extensions import Literal

from .. import fs
from ..event import CancellationToken, Event, raise_if_cancelled
from ..fs import FilePredicate, IfDirectoryExists, IfExists, Pathish

_AR_OPS_POOL = ThreadPoolExecutor(4)

T = TypeVar('T')


class ExtractInfo(NamedTuple):
    """
    Information about the extraction of a file from an archive.
    """
    #: Path within the archive from where the item was extracted
    ar_path: PurePosixPath
    #: Path on the system to where the item was extracted
    dest_path: Path
    #: The index of the file in the extraction process.
    n: int
    #: The total number of files that will be extracted.
    total_count: int


class AddInfo(NamedTuple):
    """
    Information about the addition of a file into an archive.
    """
    ar_path: PurePosixPath
    "Path within the archive that the file is being written to"


ExtractFileEvent = Event[ExtractInfo]
AddFileEvent = Event[AddInfo]


def ar_thread_pool() -> ThreadPoolExecutor:
    """
    The thread pool used to execute archive operations
    """
    return _AR_OPS_POOL


def _run_ar_op(fn: Callable[[], T]) -> Awaitable[T]:
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(ar_thread_pool(), fn)


Format = Literal['zip', 'tgz', 'tbz', 'txz']
FormatOrAuto = Literal[Format, 'auto']


def is_tar_format(fmt: Format) -> bool:
    """Return ``True`` iff this archive is a tarfile format archive."""
    return fmt in ('tgz', 'tbz', 'txz')


class _Mode(enum.Enum):
    """
    The open-mode for an archive.
    """
    #: Write a new archive file (truncates a previous archive)
    WriteNew = 'write-new'
    #: Read an existing archive
    Read = 'read'
    #: Append to an existing archive
    Append = 'append'


def _open_zip(path: Path, mode: _Mode) -> zipfile.ZipFile:
    """Return an opened ZipFile"""
    mode_str = {
        _Mode.Append: 'a',
        _Mode.WriteNew: 'w',
        _Mode.Read: 'r',
    }[mode]
    mode_str = cast(Literal['a'], mode_str)
    return zipfile.ZipFile(str(path), mode_str, zipfile.ZIP_DEFLATED)


def _open_tar(path: Path, mode: _Mode, format_: Format) -> tarfile.TarFile:
    """Return an opened TarFile with the appropriate compression"""
    algo_str = {
        'tgz': 'gz',
        'tbz': 'bz2',
        'txz': 'xz',
    }[format_]
    mode_str = {
        _Mode.Read: 'r:' + algo_str,
        _Mode.Append: 'a:',
        _Mode.WriteNew: 'w:' + algo_str,
    }[mode]
    return tarfile.open(str(path), mode_str)


class MemberFromFile(NamedTuple):
    """
    Define an archive member by copying a file from the system into the archive.
    """
    #: The path on the system to copy from
    path: Pathish
    #: The destination for the member within the archive structure
    ar_path: Pathish


class MemberFromBytes(NamedTuple):
    """
    Define an archive member by writing a buffer or string into the archive.
    """
    #: The path within the archive to store to
    ar_path: Pathish
    #: The content of the archive member. Should be `bytes` or
    #: `str`. `str` will be encoded as UTF-8.
    content: str | bytes


NewMember = Union[MemberFromFile, MemberFromBytes]


def _add_to_tar(tf: tarfile.TarFile, item: NewMember, on_add: AddFileEvent | None) -> None:
    """Add the given item to a tar archive file"""
    if on_add:
        on_add.emit(AddInfo(PurePosixPath(item.ar_path)))
    if isinstance(item, MemberFromFile):
        tf.add(str(item.path), str(item.ar_path) if item.ar_path else None)
    else:
        assert isinstance(item, MemberFromBytes), f'Invalid item for tar archive {item!r}'
        info = tarfile.TarInfo(str(item.ar_path))
        buf = item.content
        if isinstance(buf, str):
            buf = buf.encode('utf-8')
        info.size = len(buf)
        bio = io.BytesIO(buf)
        tf.addfile(info, bio)


def _create_tar(path: Path, items: Iterable[NewMember], format: Format, on_add: AddFileEvent | None,
                cancel: CancellationToken | None) -> Path:
    """Create a tar archive for the given items."""
    with _open_tar(path, _Mode.WriteNew, format) as tfd:
        for item in items:
            raise_if_cancelled(cancel)
            _add_to_tar(tfd, item, on_add)
    return path


def _add_to_zip(zfd: zipfile.ZipFile, item: NewMember, on_add: AddFileEvent | None) -> None:
    """Add a single item to a Zip archive"""
    if on_add:
        on_add.emit(AddInfo(PurePosixPath(item.ar_path)))
    if isinstance(item, MemberFromFile):
        zfd.write(str(item.path), str(item.ar_path) if item.ar_path else None)
    else:
        assert isinstance(item, MemberFromBytes), f'Invalid item for zip archive {item!r}'
        buf = item.content
        zfd.writestr(str(item.ar_path), buf)


def _create_zip(path: Path, items: Iterable[NewMember], on_add: AddFileEvent | None,
                cancel: CancellationToken | None) -> Path:
    """Create a zip archive for the given items"""
    with _open_zip(path, _Mode.WriteNew) as zfd:
        for item in items:
            raise_if_cancelled(cancel)
            _add_to_zip(zfd, item, on_add)
    return path


def _get_format(path: Path, format_: FormatOrAuto) -> Format:
    """Get the format to use with ``path``"""
    if format_ != 'auto':
        return format_
    if path.name.endswith('.zip'):
        return 'zip'
    if path.name.endswith('.tar.gz') or path.suffix in ('.tgz', '.tar'):
        return 'tgz'
    if path.name.endswith('.tar.bz2') or path.suffix in ('.tbz', '.tbz2'):
        return 'tbz'
    if path.name.endswith('.tar.xz') or path.suffix in ('.txz'):
        return 'txz'
    raise RuntimeError(f'Unable to guess archive format from the filename ({path.name})')


async def create(path: Pathish,
                 items: Iterable[NewMember],
                 *,
                 format: FormatOrAuto = 'auto',
                 on_add: AddFileEvent | None = None,
                 cancel: CancellationToken | None = None) -> Path:
    """
    Create an archive.

    :param path: The full file path for the destination of the archive.
    :param items: The items that should be put in the archive. Refer to
        :class:`MemberFromFile` and :class:`MemberFromBytes` to understand this
        parameter.
    :param format: The format of archive to create. If :attr:`.Guess`, the type
        of archive will be guessed from the file extension of ``path``.
    :param cancel: A cancellation token for the archive creation.

    :returns: The path to the generated archive.
    """
    path = Path(path)
    format = _get_format(path, format)

    if is_tar_format(format):
        return await _run_ar_op(lambda: _create_tar(Path(path), items, cast(Format, format), on_add, cancel))
    if format == 'zip':
        return await _run_ar_op(lambda: _create_zip(Path(path), items, on_add, cancel))
    raise RuntimeError(f'Invalid `format` given ({repr(format)})')


def dir_items(root: Path,
              *,
              only_if: FilePredicate | None = None,
              unless: FilePredicate | None = None,
              prefix: Pathish | None = None) -> Iterable[MemberFromFile]:
    """
    Return a single-pass iterable of :class:`MemberFromFile` objects.

    :param root: The directory to enumerate. Archive member paths will be
        relative to this directory.
    :param only_if: Predicate to filter files to add.
    :param unless: Predicate to filter files to exclude.
    :param prefix: If provided, this directory prefix will be prepended to
        every item's path within the archive.

    .. note:: ``only_if`` is evaluated before ``unless``.
    """
    for item in root.glob('**/*'):
        if only_if and not only_if(item):
            continue
        if unless and unless(item):
            continue
        relpath = PurePosixPath(item.relative_to(root).as_posix())
        ar_path = relpath
        if prefix is not None:
            ar_path = PurePosixPath(prefix) / ar_path
        yield MemberFromFile(path=item, ar_path=ar_path)


async def create_from_dir(dirpath: Pathish,
                          *,
                          destination: Pathish | None = None,
                          only_if: FilePredicate | None = None,
                          unless: FilePredicate | None = None,
                          prefix: PurePosixPath | str | None = None,
                          format: FormatOrAuto = 'auto',
                          cancel: CancellationToken | None = None) -> Path:
    """
    Create an archive from the contents of a directory.

    :param dirpath: The directory to archive. The contents of this directory
        will be added to the archive, and the paths within the archive will
        be relative to this directory.
    :param destination: The destination of the archive. If not given, will
        place the archive as a sibling of ``dirpath`` with a file extension
        appended to the filename.
    :param only_if: A predicate of which files to add.
    :param unless: A predicate of which files to exclude.
    :param prefix: If given, this will be created as a top-level directory to
        prefix all files in the archive.
    :param format: The format to use. If not given, deduced from
        ``destination``. If neither is given, will create a ``.tgz`` archive.
    :param cancel: A cancellation token to interrupt the archive creation.
    """
    dirpath = Path(dirpath).absolute()
    if destination is None:
        default_ext = {
            'auto': '.tgz',
            'tgz': '.tgz',
            'txz': '.txz',
            'tbz': '.tbz',
            'zip': '.zip',
        }[format]
        destination = dirpath.parent / (dirpath.name + default_ext)
    destination = destination or (dirpath.parent / (dirpath.name + '.tgz'))
    return await create(destination,
                        dir_items(
                            dirpath,
                            only_if=only_if,
                            unless=unless,
                            prefix=PurePosixPath(prefix) if prefix else None,
                        ),
                        format=format,
                        cancel=cancel)


async def _prep_expand(dest: Path, if_exists: IfDirectoryExists, cancel: CancellationToken | None) -> bool:
    """Prepare a destination for archive expansion"""
    if not dest.exists():
        # Destination isn't already there. Create the directory in preparation for expansion
        dest.mkdir(parents=True, exist_ok=True)
        return True
    # Decide what to do next, since the destination directory exists
    if if_exists == 'replace':
        # We're replacing the directory, so remove the existing contents completely
        await fs.remove(dest, recurse=True, cancel=cancel)
        return True
    if if_exists == 'keep':
        # We're going to keep the destination and not perform archive expansion
        return False
    if if_exists == 'merge':
        # We're merging over the top, so keep the top-level directory as-is
        return True
    if if_exists == 'fail':
        # We don't expect it to already exist, so raise an error
        raise FileExistsError(f'Archive expansion destination ({dest}) already exists')
    # Unreachable:
    assert False, f'Invalid value for if_exists: {repr(if_exists)}'


def _write_file(fd: IO[bytes], dest: Path) -> None:
    """
    Write the bytes of an IO-like object to the given path. Used by the
    archive expansion to write an archive member to the filesystem.
    """
    dest.parent.mkdir(exist_ok=True, parents=True)
    with dest.open('wb') as out:
        while True:
            buf = fd.read(1024 * 1)
            if not buf:
                break
            out.write(buf)


MemInfo = Union[tarfile.TarInfo, zipfile.ZipInfo]


def _path_of(m: MemInfo) -> Path:
    if isinstance(m, tarfile.TarInfo):
        return Path(m.name)
    return Path(m.filename)


def _is_dir(m: MemInfo) -> bool:
    if isinstance(m, tarfile.TarInfo):
        return m.isdir()
    return m.is_dir()


def _is_file(m: MemInfo) -> bool:
    if isinstance(m, tarfile.TarInfo):
        return m.isfile()
    return not m.is_dir()


def _open(ar: zipfile.ZipFile | tarfile.TarFile, m: MemInfo) -> IO[bytes]:
    if isinstance(ar, tarfile.TarFile):
        assert isinstance(m, tarfile.TarInfo)
        mem = ar.extractfile(m)
        assert mem
        return mem
    assert isinstance(m, zipfile.ZipInfo)
    return ar.open(m)


async def _expand_archive(
    archive: zipfile.ZipFile | tarfile.TarFile,
    dest: Path,
    only_if: FilePredicate,
    unless: FilePredicate,
    strip_components: int,
    if_file_exists: IfExists,
    cancel: CancellationToken | None,
    on_extract: ExtractFileEvent | None,
) -> Path:
    # Switch based on archive type
    is_tar = isinstance(archive, tarfile.TarFile)

    all_members = cast(Sequence[Union[zipfile.ZipInfo, tarfile.TarInfo]],
                       archive.getmembers() if is_tar else archive.infolist())  # type: ignore
    filtered = (mem for mem in all_members if (only_if(_path_of(mem)) and not unless(_path_of(mem))))

    dest_map: list[tuple[Union[zipfile.ZipInfo, tarfile.TarInfo], Path]] = []

    for member in filtered:
        path_elems = PurePosixPath(_path_of(member)).parts
        new_elems = path_elems[min(strip_components, len(path_elems)):]
        if not len(new_elems):
            continue
        new_dest = functools.reduce(lambda a, b: a / b, new_elems, dest)
        if new_dest.exists():
            if if_file_exists == 'fail':
                raise FileExistsError(f'Destination file already exists: {new_dest}')
            if if_file_exists == 'keep':
                # Skip this archive member
                continue
            if if_file_exists == 'replace':
                if _is_dir(member):
                    # We keep around directories, since we want to merge over the top of them
                    pass
                else:
                    os.unlink(new_dest)
        dest_map.append((member, new_dest))

    dirs: list[tuple[tarfile.TarInfo, Path]] = []

    for idx, pair in enumerate(dest_map):
        raise_if_cancelled(cancel)
        mem, dest_filepath = pair
        if on_extract:
            info = ExtractInfo(PurePosixPath(_path_of(mem)), dest_filepath, idx, len(dest_map))
            on_extract.emit(info)
        if _is_file(mem):
            fd = _open(archive, mem)
            assert fd
            await _run_ar_op(lambda: _write_file(fd, dest_filepath))  # pylint: disable=cell-var-from-loop
        if _is_dir(mem) and is_tar:
            assert isinstance(mem, tarfile.TarInfo)
            dirs.append((mem, Path(dest_filepath)))

    dirs.sort(key=lambda pair: pair[1])
    dirs.reverse()
    for mem, dirpath in dirs:
        assert is_tar, 'Code path should not be taken unless expanding a Tar archive'
        os.chmod(dirpath, mem.mode)
        os.utime(dirpath, (mem.mtime, mem.mtime))

    return dest


async def expand(path: Pathish,
                 *,
                 destination: Pathish,
                 only_if: FilePredicate | None = None,
                 unless: FilePredicate | None = None,
                 strip_components: int = 0,
                 format: FormatOrAuto = 'auto',
                 if_exists: IfDirectoryExists = 'fail',
                 if_file_exists: IfExists = 'fail',
                 cancel: CancellationToken | None = None,
                 on_extract: ExtractFileEvent | None = None) -> Path:
    """
    Expand the contents of an archive into a directory.

    :param path: The path to an archive.
    :param destination: The directory to expand *into*. The top-level items of
        the archive will appear items within this directory.
    :param only_if: A predicate of what files to include.
    :param unless: A predicate of what files to exclude.
    :param strip_components: Number of leading path components to remove when
        resolving the destination of a file from the archive. By default, the
        entire relative path from the archive root will be appended to
        ``destination``. This open will strip directory components from that
        archive path before appending it to ``destination``. If the number of
        path elements is less than or equal to ``strip_components``, the file
        will be dropped.
    :param format: The archive format. May be guessed from the file extension of
        the archive.
    :param if_exists: The action to perform if the destination directory exists.
    :param if_file_exists: The action to perform if the extraction of an
        archive member collides with an existing file in the destination.
    :param cancel: A cancellation token for the operation.
    :param on_extract: An event handler that receives an event whenever an
        archive member is extracted.
    :returns: The path referred to by ``destination``.

    .. note:: ``only_if`` is evaluated before ``unless``.
    """
    format = _get_format(Path(path), format)

    assert if_file_exists != 'merge', '"merge" is not valid for if_file_exists'

    destination = Path(destination)

    if not await _prep_expand(destination, if_exists, cancel):
        # We want to keep the contents
        return destination

    archive: Union[zipfile.ZipFile, tarfile.TarFile]
    if is_tar_format(format):
        archive = tarfile.open(path, 'r:*')
    elif format == 'zip':
        archive = zipfile.ZipFile(path, 'r')
    else:
        raise RuntimeError(f'Unknown archive format spec: {repr(format)}')

    return await _expand_archive(
        archive,
        destination,
        only_if or (lambda _: True),
        unless or (lambda _: False),
        strip_components,
        if_file_exists,
        cancel,
        on_extract,
    )
