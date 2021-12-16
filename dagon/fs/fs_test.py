from __future__ import annotations

from pathlib import Path
import pytest
import py.path

from dagon import fs

async_test = pytest.mark.asyncio


@async_test
async def test_copy_file(tmpdir: py.path.local) -> None:
    tmpdir.join('foo.txt').write_binary(b'hello')
    await fs.copy_file(tmpdir / 'foo.txt', tmpdir / 'bar.txt')
    assert tmpdir.join('bar.txt').read_binary() == b'hello'


@async_test
async def test_copy_noexist_file(tmpdir: py.path.local) -> None:
    with pytest.raises(FileNotFoundError):
        await fs.copy_file(tmpdir.join('nope.txt'), 'nope.txt')


@async_test
async def test_copy_to_noexist_dir(tmpdir: py.path.local) -> None:
    tmpdir.join('foo.txt').write_binary(b'hi')
    tmpdir.join('bar.txt').write_binary(b'yo')
    with pytest.raises(FileNotFoundError):
        await fs.copy_file(tmpdir.join('foo.txt'), tmpdir.join('a/b'), mkdirs=False)
    with pytest.raises(NotADirectoryError):
        await fs.copy_file(tmpdir.join('foo.txt'), tmpdir.join('bar.txt/b'), mkdirs=False)
    with pytest.raises(NotADirectoryError):
        await fs.copy_file(tmpdir.join('foo.txt'), tmpdir.join('bar.txt/b'), mkdirs=True)
    # Okay: Creates the directory:
    await fs.copy_file(tmpdir.join('foo.txt'), tmpdir.join('a/b'), mkdirs=True)
    with pytest.raises(IsADirectoryError):
        await fs.copy_file(tmpdir.join('a'), tmpdir.join('b'))


@async_test
async def test_copy_file_is_dir(tmpdir: py.path.local) -> None:
    tmpdir.join('foo.dir').mkdir()
    with pytest.raises(IsADirectoryError):
        await fs.copy_file(tmpdir.join('foo.dir'), tmpdir.join('bar.dir'))


def test_iter_pathish() -> None:
    assert list(fs.iter_pathish('foo')) == [Path('foo')]
    assert list(fs.iter_pathish('foo', type=str)) == ['foo']
    assert list(fs.iter_pathish(['foo'], type=str)) == ['foo']
    assert list(fs.iter_pathish(['foo', 'bar'], type=str)) == ['foo', 'bar']


@async_test
async def test_create_tree(tmpdir: py.path.local) -> None:
    await fs.create_tree(tmpdir, {
        'a.txt': b'Hello!',
        'b.txt': b'Goodbye!',
        'c': {
            'inner.txt': b'Howdy',
        },
    })
    assert tmpdir.join('a.txt').read_binary() == b'Hello!'
    assert tmpdir.join('b.txt').read_binary() == b'Goodbye!'
    assert tmpdir.join('c/inner.txt').read_binary() == b'Howdy'


@async_test
async def test_copy_tree(tmpdir: py.path.local) -> None:
    await fs.create_tree(tmpdir.join('base'), {
        'a': b'a',
        'b': b'b',
        'c': {
            'd': {
                'e': b'e',
            },
        },
    })
    await fs.copy_tree(tmpdir / 'base', tmpdir / 'target')
    assert tmpdir.join('base').isdir()  # Directory still exists
    assert tmpdir.join('target').isdir()  # Directory was created
    assert tmpdir.join('target/c/d/e').read_binary() == b'e'


@async_test
async def test_dir_merge(tmpdir: py.path.local) -> None:
    await fs.create_tree(tmpdir, {
        'a': {
            'file.a.txt': b'foo'
        },
        'b': {
            'file.b.txt': b'bar'
        },
    })
    await fs.copy_tree(tmpdir / 'a', tmpdir / 'c')
    await fs.copy_tree(tmpdir / 'b', tmpdir / 'c', if_exists='merge')
    assert tmpdir.join('c/file.a.txt').read_binary() == b'foo'
    assert tmpdir.join('c/file.b.txt').read_binary() == b'bar'
    tmpdir.join('a/file.a.txt').write_binary(b'baz')
    await fs.copy_tree(tmpdir / 'a', tmpdir / 'c', if_file_exists='keep', if_exists='merge')
    assert tmpdir.join('c/file.a.txt').read_binary() == b'foo'
    await fs.copy_tree(tmpdir / 'a', tmpdir / 'c', if_file_exists='replace', if_exists='merge')
    assert tmpdir.join('c/file.a.txt').read_binary() == b'baz'
