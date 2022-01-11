import tarfile
import tempfile
import zipfile
from pathlib import Path

import pytest

from dagon import ar

PKG_ROOT = Path(__file__).absolute().parent.parent


@pytest.mark.asyncio
async def test_tar_create_expand() -> None:
    tdir = tempfile.mkdtemp()
    tar_path = await ar.create_from_dir(PKG_ROOT, destination=Path(tdir) / 'test.tgz')
    assert tar_path.is_file()
    print('Created tar at', tar_path)

    with tarfile.open(tar_path) as tfd:
        tl_py = tfd.getmember('ar/ar_test.py')
        assert tl_py.isfile()
        tl_py_fd = tfd.extractfile(tl_py)
        assert tl_py_fd
        exp_content = PKG_ROOT.joinpath('ar/ar_test.py').read_bytes()
        assert tl_py_fd.read() == exp_content

    exp_dir = Path(tempfile.mkdtemp())
    await ar.expand(tar_path, destination=exp_dir, if_exists='merge')
    assert (exp_dir / 'ar/ar_test.py').is_file()

    # Trying to expand again will raise
    with pytest.raises(FileExistsError):
        await ar.expand(tar_path, destination=exp_dir)

    # Merge mode will complain about overwriting files
    with pytest.raises(FileExistsError):
        await ar.expand(tar_path, destination=exp_dir, if_exists='merge')

    await ar.expand(tar_path, destination=exp_dir, if_exists='merge', if_file_exists='replace')

    await ar.expand(tar_path, destination=exp_dir, if_exists='merge', if_file_exists='keep')


@pytest.mark.asyncio
async def test_zip_create_expand() -> None:
    # Check that the default extension is .zip when the format is Zip
    zip_path = await ar.create_from_dir(PKG_ROOT, format='zip')
    assert zip_path.suffix == '.zip'
    await ar.fs.remove(zip_path)

    tdir = Path(tempfile.mkdtemp())
    zip_path = await ar.create_from_dir(PKG_ROOT, destination=tdir / 'test.zip', format='zip')

    assert zip_path.exists()

    with zipfile.ZipFile(zip_path) as zfd:
        tl_py = zfd.getinfo('ar/ar_test.py')
        content = (PKG_ROOT / 'ar/ar_test.py').read_bytes()
        assert zfd.read(tl_py) == content

    exp_dir = Path(tempfile.mkdtemp())
    await ar.expand(zip_path, destination=exp_dir, if_exists='merge')
    assert (exp_dir / 'ar/ar_test.py').is_file()

    # Trying to expand again will raise
    with pytest.raises(FileExistsError):
        await ar.expand(zip_path, destination=exp_dir)

    # Merge mode will complain about overwriting files
    with pytest.raises(FileExistsError):
        await ar.expand(zip_path, destination=exp_dir, if_exists='merge')

    await ar.expand(zip_path, destination=exp_dir, if_exists='merge', if_file_exists='replace')

    await ar.expand(zip_path, destination=exp_dir, if_exists='merge', if_file_exists='keep')
