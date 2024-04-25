import fsspec

from utils import CONSTANTS


def test_fsspec():
    fs = fsspec.filesystem("xet")
    assert str(fs.open(CONSTANTS.TITANIC_MAIN +
                       '/readme.md').readline()).startswith("b'#")
    assert str(fs.open(CONSTANTS.TITANIC_MAIN +
                       '/readme.md').readline()).startswith("b'#")

    # https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.glob
    listing = fs.glob(CONSTANTS.TITANIC_MAIN)
    print(listing)
    assert len(listing) == 1
    assert listing == ['xdssio/titanic/main']

    listing = fs.glob(CONSTANTS.TITANIC_MAIN + '/data')
    print(listing)
    assert len(listing) == 1
    assert listing == ['xdssio/titanic/main/data']

    listing = fs.glob(CONSTANTS.TITANIC_MAIN + '/data/')
    print(listing)
    assert len(listing) == 1
    assert listing == ['xdssio/titanic/main/data/']

    listing = fs.glob(CONSTANTS.TITANIC_MAIN + '/data/*')
    print(listing)
    assert len(listing) == 2
    assert listing == ['xdssio/titanic/main/data/titanic_0.parquet', 'xdssio/titanic/main/data/titanic_1.parquet']

    listing = fs.glob(CONSTANTS.TITANIC_MAIN + '/data/*parquet')
    print(listing)
    assert len(listing) == 2
    assert listing == ['xdssio/titanic/main/data/titanic_0.parquet', 'xdssio/titanic/main/data/titanic_1.parquet']

    listing = fs.glob(CONSTANTS.TITANIC_MAIN + '/data/titanic*')
    print(listing)
    assert len(listing) == 2
    assert listing == ['xdssio/titanic/main/data/titanic_0.parquet', 'xdssio/titanic/main/data/titanic_1.parquet']

    listing = fs.glob(CONSTANTS.TITANIC_MAIN + '/data/*1.parquet')
    print(listing)
    assert len(listing) == 1
    assert listing == ['xdssio/titanic/main/data/titanic_1.parquet']

    listing = fs.glob(CONSTANTS.TITANIC_MAIN + '/**/*parquet')
    print(listing)
    assert len(listing) == 3
    assert listing == ['xdssio/titanic/main/data/titanic_0.parquet', 'xdssio/titanic/main/data/titanic_1.parquet', 'xdssio/titanic/main/titanic.parquet']

    listing = fs.glob(CONSTANTS.TITANIC_MAIN + '/titanic*')
    print(listing)
    assert len(listing) == 3
    assert listing == ['xdssio/titanic/main/titanic.csv', 'xdssio/titanic/main/titanic.json',
                       'xdssio/titanic/main/titanic.parquet']
