import io

import pytest

import pyxet
from utils import skip_if_no, CONSTANTS


@pytest.mark.skip("Not sure if pyxet will implement read_arrow - TODO")
def test_read_arrow():
    dataset = pyxet.read_arrow(CONSTANTS.TITANIC_PARQUET)
    assert dataset.to_table().num_rows == 891


@skip_if_no("pyarrow")
def test_pyarrow_dataset():
    import pyarrow.dataset as ds
    fs = pyxet.XetFS(repo_url=CONSTANTS.TITANIC_MAIN)
    dataset = ds.dataset(CONSTANTS.TITANIC_PARQUET, filesystem=fs)
    assert dataset.to_table().num_rows == 891


@skip_if_no("pyarrow")
def test_pyarrow_parquet():
    from pyarrow.parquet import ParquetFile
    import pyarrow as pa

    pf = ParquetFile(CONSTANTS.TITANIC_PARQUET,
                     filesystem=pyxet.XetFS(repo_url=CONSTANTS.TITANIC_MAIN))
    first_ten_rows = next(pf.iter_batches(batch_size=10))
    df = pa.Table.from_batches([first_ten_rows]).to_pandas()
    assert df.shape == (10, 12)


@skip_if_no("pyarrow")
def test_pyarrow_stream():
    import pandas as pd
    from pyarrow.fs import PyFileSystem, FSSpecHandler

    pa_fs = PyFileSystem(FSSpecHandler(
        pyxet.XetFS(repo_url=CONSTANTS.TITANIC_MAIN)))
    with pa_fs.open_input_stream(CONSTANTS.TITANIC_CSV) as stream:
        df = pd.read_csv(io.BytesIO(stream.readall()))
    assert df.shape == (891, 12)


@pytest.mark.skip("cp not implemented")
def test_pyarrow_stream_cp():
    with pytest.raises(NotImplementedError):  # TODO
        pa_fs.copy_file(CONSTANTS.TITANIC_CSV,
                        'https://xethub.com/xdssio/titanic.git/main/titanic2.csv')


@pytest.mark.skip("Not sure if we need this - TODO")
def test_pyarrow_fsspec():
    from pyarrow import fs
    from fsspec.implementations.arrow import ArrowFSWrapper

    local = fs.LocalFileSystem()
    local_fsspec = ArrowFSWrapper(local)
    local_fsspec.ls('pyxet')
