from __future__ import annotations

from pathlib import Path
import pytest

from dagon import fs

async_test = pytest.mark.asyncio


@async_test
async def test_copy_file(tmp_path: Path) -> None:
    tmp_path.joinpath('foo.txt').write_bytes(b'hello')
    await fs.copy_file(tmp_path / 'foo.txt', tmp_path / 'bar.txt')
    assert tmp_path.joinpath('bar.txt').read_bytes() == b'hello'


@async_test
async def test_copy_noexist_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        await fs.copy_file(tmp_path.joinpath('nope.txt'), 'nope.txt')


@async_test
async def test_copy_to_noexist_dir(tmp_path: Path) -> None:
    tmp_path.joinpath('foo.txt').write_bytes(b'hi')
    tmp_path.joinpath('bar.txt').write_bytes(b'yo')
    with pytest.raises(FileNotFoundError):
        await fs.copy_file(tmp_path.joinpath('foo.txt'), tmp_path.joinpath('a/b'), mkdirs=False)
    with pytest.raises(NotADirectoryError):
        await fs.copy_file(tmp_path.joinpath('foo.txt'), tmp_path.joinpath('bar.txt/b'), mkdirs=False)
    with pytest.raises(NotADirectoryError):
        await fs.copy_file(tmp_path.joinpath('foo.txt'), tmp_path.joinpath('bar.txt/b'), mkdirs=True)
    # Okay: Creates the directory:
    await fs.copy_file(tmp_path.joinpath('foo.txt'), tmp_path.joinpath('a/b'), mkdirs=True)
    with pytest.raises(IsADirectoryError):
        await fs.copy_file(tmp_path.joinpath('a'), tmp_path.joinpath('b'))


@async_test
async def test_remove_file(tmp_path: Path) -> None:
    foo_txt = tmp_path / 'foo.txt'

    with pytest.raises(FileNotFoundError):
        await fs.remove(foo_txt)
    await fs.remove(foo_txt, absent_ok=True)

    foo_txt.write_text('hello')
    assert foo_txt.is_file()

    await fs.remove(foo_txt)
    assert not foo_txt.is_file()

    with pytest.raises(FileNotFoundError):
        await fs.remove(foo_txt)
    await fs.remove(foo_txt, absent_ok=True)

    foo_txt.write_text('hello')
    assert foo_txt.is_file()
    await fs.remove(foo_txt, absent_ok=True)
    assert not foo_txt.is_file()

    foo_txt.write_text('hello')
    await fs.remove(foo_txt, absent_ok=True, recurse=True)
    assert not foo_txt.is_file()


@async_test
async def test_remove_file_fast(tmp_path: Path) -> None:
    foo_txt = tmp_path / 'foo.txt'
    foo_txt.write_text('hello')
    assert foo_txt.exists()
    # File is already deleted, even if we don't await it yet:
    f = fs.remove(foo_txt)
    assert not foo_txt.exists()
    await f


@async_test
async def test_remove_tree(tmp_path: Path) -> None:
    subdir = tmp_path / 'foo'
    with pytest.raises(FileNotFoundError):
        await fs.remove(subdir)
    await fs.remove(subdir, recurse=True, absent_ok=True)
    await fs.create_tree(
        tmp_path,
        {'foo': {
            'bar.bin': b'hello',
            'baz.bin': b'hola',
            'quux': {
                'info.txt': b'I am a file in a subdirectory'
            }
        }})
    assert subdir.is_dir()
    f = fs.remove(subdir, recurse=True)
    assert not subdir.is_dir()
    await f


@async_test
async def test_copy_file_is_dir(tmp_path: Path) -> None:
    tmp_path.joinpath('foo.dir').mkdir()
    with pytest.raises(IsADirectoryError):
        await fs.copy_file(tmp_path.joinpath('foo.dir'), tmp_path.joinpath('bar.dir'))


def test_iter_pathish() -> None:
    assert list(fs.iter_pathish('foo')) == [Path('foo')]
    assert list(fs.iter_pathish('foo', fac=str)) == ['foo']
    assert list(fs.iter_pathish(['foo'], fac=str)) == ['foo']
    assert list(fs.iter_pathish(['foo', 'bar'], fac=str)) == ['foo', 'bar']


@async_test
async def test_create_tree(tmp_path: Path) -> None:
    await fs.create_tree(tmp_path, {
        'a.txt': b'Hello!',
        'b.txt': b'Goodbye!',
        'c': {
            'inner.txt': b'Howdy',
        },
    })
    assert tmp_path.joinpath('a.txt').read_bytes() == b'Hello!'
    assert tmp_path.joinpath('b.txt').read_bytes() == b'Goodbye!'
    assert tmp_path.joinpath('c/inner.txt').read_bytes() == b'Howdy'


@async_test
async def test_copy_tree(tmp_path: Path) -> None:
    await fs.create_tree(tmp_path.joinpath('base'), {
        'a': b'a',
        'b': b'b',
        'c': {
            'd': {
                'e': b'e',
            },
        },
    })
    await fs.copy_tree(tmp_path / 'base', tmp_path / 'target')
    assert tmp_path.joinpath('base').is_dir()  # Directory still exists
    assert tmp_path.joinpath('target').is_dir()  # Directory was created
    assert tmp_path.joinpath('target/c/d/e').read_bytes() == b'e'


@async_test
async def test_dir_merge(tmp_path: Path) -> None:
    await fs.create_tree(tmp_path, {
        'a': {
            'file.a.txt': b'foo'
        },
        'b': {
            'file.b.txt': b'bar'
        },
    })
    await fs.copy_tree(tmp_path / 'a', tmp_path / 'c')
    await fs.copy_tree(tmp_path / 'b', tmp_path / 'c', if_exists='merge')
    assert tmp_path.joinpath('c/file.a.txt').read_bytes() == b'foo'
    assert tmp_path.joinpath('c/file.b.txt').read_bytes() == b'bar'
    tmp_path.joinpath('a/file.a.txt').write_bytes(b'baz')
    await fs.copy_tree(tmp_path / 'a', tmp_path / 'c', if_file_exists='keep', if_exists='merge')
    assert tmp_path.joinpath('c/file.a.txt').read_bytes() == b'foo'
    await fs.copy_tree(tmp_path / 'a', tmp_path / 'c', if_file_exists='replace', if_exists='merge')
    assert tmp_path.joinpath('c/file.a.txt').read_bytes() == b'baz'
