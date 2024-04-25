import hashlib
import pickle
import time

import cloudpickle
import pandas as pd
import pytest

import pyxet
from utils import CONSTANTS


class Model:
    def __init__(self, a):
        self.a = a


def test_open_file_mode():
    with pyxet.open(CONSTANTS.TITANIC_JSON, mode='rb') as f:
        data = f.readall()
    assert type(data) == bytes
    # really there is no difference between 'r' and 'rb'
    with pyxet.open(CONSTANTS.TITANIC_JSON, mode='r') as f:
        data = f.read()
    assert type(data) == bytes


def test_open_file():
    f = pyxet.open(CONSTANTS.TITANIC_CSV)
    row = f.readline().decode()
    assert len(row.split(",")) == 12
    text = f.readall().decode()
    rows = text.split('\n')
    assert len(rows) == 892


def test_open_file_with_context_manager():
    with pyxet.open(CONSTANTS.TITANIC_CSV) as f:
        row = f.readline().decode()
        assert len(row.split(",")) == 12
        text = f.readall().decode()
        rows = text.split('\n')
        assert len(rows) == 892


def test_open_file_direct():
    csv = pd.read_csv(CONSTANTS.TITANIC_XET_CSV)
    assert csv.shape == (891, 12)


def test_ls():
    fs = pyxet.XetFS()
    assert len(fs.ls(CONSTANTS.TITANIC_MAIN)) == 6
    assert len(fs.ls(CONSTANTS.TITANIC_MAIN, detail=True)[0]) == 3
    assert len(fs.ls(CONSTANTS.TITANIC_MAIN + '/data')) == 2
    print(fs.ls(CONSTANTS.TITANIC_MAIN + '/data'))


def test_stat():
    fs = pyxet.XetFS()
    stat = fs.stat('xdssio/titanic/main/data/titanic_0.parquet')
    print(stat)
    assert (stat['name'] == 'xdssio/titanic/main/data/titanic_0.parquet')


def test_info():
    fs = pyxet.XetFS()
    stat = fs.info('xdssio/titanic/main/data/titanic_0.parquet')
    print(stat)
    assert (stat['name'] == 'xdssio/titanic/main/data/titanic_0.parquet')


def test_actual_read_write():
    # we do 1 test for read/write
    fs = pyxet.XetFS()
    pyxet.login(CONSTANTS.TESTING_USERNAME, CONSTANTS.TESTING_TOKEN, email="a@a.com")
    test_data = hashlib.md5(str(time.time()).encode('utf-8')).hexdigest()

    with fs.transaction as tr:
        tr.set_commit_message("test_transaction")
        f = fs.open(CONSTANTS.TESTING_TEMPREPO + "/test_data.dat", "w")
        f.write(test_data)
        f.close()

    f = fs.open(CONSTANTS.TESTING_TEMPREPO + "/test_data.dat", "r")
    assert f.read() == test_data


def test_fake_write_model_pickle():
    model = Model(1)
    fs = pyxet.XetFS()
    pyxet.login(CONSTANTS.TESTING_USERNAME, CONSTANTS.TESTING_TOKEN, email="a@a.com")
    file_path = CONSTANTS.TESTING_TEMPREPO + '/a/b/model.pickle'
    with fs.transaction:
        fs.set_commit_message("upload pickle model")
        pickle.dump(model, fs.open(file_path, 'wb')._fake_writes())
        fs.transaction._set_do_not_commit()
        clist = fs.transaction.get_change_list()
        assert (len(clist['new_files']) == 1)
        assert (len(clist['deletes']) == 0)
        assert (len(clist['copies']) == 0)

    assert (fs.intrans is False)


def test_fake_write_model_cloudpickle():
    model = Model(3)
    fs = pyxet.XetFS()
    pyxet.login(CONSTANTS.TESTING_USERNAME, CONSTANTS.TESTING_TOKEN, email="a@a.com")
    file_path = CONSTANTS.TESTING_TEMPREPO + '/a/b/model_cloud.pickle'
    with fs.transaction:
        fs.set_commit_message("upload cloudpickle model")
        cloudpickle.dump(model, fs.open(file_path, 'wb')._fake_writes())
        fs.transaction._set_do_not_commit()
        clist = fs.transaction.get_change_list()
        assert (len(clist['new_files']) == 1)
        assert (len(clist['deletes']) == 0)
        assert (len(clist['copies']) == 0)
    assert (fs.intrans is False)


def test_fake_write_model_cloudpickle_with_explicit_transaction():
    model = Model(4)
    fs = pyxet.XetFS()
    pyxet.login(CONSTANTS.TESTING_USERNAME, CONSTANTS.TESTING_TOKEN, email="a@a.com")
    file_path = CONSTANTS.TESTING_TEMPREPO + '/a/b/model_cloud2.pickle'
    fs.start_transaction("upload pickle model")
    cloudpickle.dump(model, fs.open(file_path, 'wb')._fake_writes())
    fs.transaction._set_do_not_commit()
    clist = fs.transaction.get_change_list()
    assert (len(clist['new_files']) == 1)
    assert (len(clist['deletes']) == 0)
    assert (len(clist['copies']) == 0)
    fs.end_transaction()
    assert (fs.intrans is False)


def test_fake_write_with_errors():
    fs = pyxet.XetFS()
    pyxet.login(CONSTANTS.TESTING_USERNAME, CONSTANTS.TESTING_TOKEN, email="a@a.com")
    fs.start_transaction("upload pickle model")
    f = fs.open(CONSTANTS.TESTING_TEMPREPO + "/test_data.dat", "w")
    f.write("hello")
    f.close()
    fs._transaction._set_error_on_commit()
    with pytest.raises(RuntimeError):
        fs.end_transaction()
    assert (fs.intrans is False)


def test_failed_transaction_in_scope():
    fs = pyxet.XetFS()
    pyxet.login(CONSTANTS.TESTING_USERNAME, CONSTANTS.TESTING_TOKEN, email="a@a.com")
    with pytest.raises(RuntimeError):
        with fs.transaction:
            f = fs.open(CONSTANTS.TESTING_TEMPREPO + "/test_data.dat", "w")
            f.write("hello")
            f.close()
            fs._transaction._set_error_on_commit()
    assert (fs.intrans is False)


def test_recursive_transaction():
    fs = pyxet.XetFS()
    pyxet.login(CONSTANTS.TESTING_USERNAME, CONSTANTS.TESTING_TOKEN, email="a@a.com")
    with fs.transaction:
        fs._transaction._set_do_not_commit()
        with pytest.raises(RuntimeError):
            with fs.transaction:
                # this is not ok
                pass


def test_copy():
    fs = pyxet.XetFS()
    pyxet.login(CONSTANTS.TESTING_USERNAME, CONSTANTS.TESTING_TOKEN, email="a@a.com")
    prefix = CONSTANTS.TESTING_TEMPREPO_ROOT
    with fs.transaction:
        fs.copy(f"{prefix}/main/test_data.dat", f"{prefix}/main/blah3")
        fs.copy(f"{prefix}/main/test_data.dat", f"{prefix}/branch/blah3")
        fs._transaction._set_do_not_commit()
        clist = fs.transaction.get_change_list()
        assert (len(clist['new_files']) == 0)
        assert (len(clist['deletes']) == 0)
        assert (len(clist['copies']) == 2)
        assert (len(fs.transaction._transaction_pool) == 2)


def test_delete():
    fs = pyxet.XetFS()
    pyxet.login(CONSTANTS.TESTING_USERNAME, CONSTANTS.TESTING_TOKEN, email="a@a.com")
    prefix = CONSTANTS.TESTING_TEMPREPO_ROOT
    with fs.transaction:
        fs.rm(f"{prefix}/main/blah")
        fs.rm(f"{prefix}/branch/blah")
        fs._transaction._set_do_not_commit()
        clist = fs.transaction.get_change_list()
        assert (len(clist['new_files']) == 0)
        assert (len(clist['deletes']) == 2)
        assert (len(fs.transaction._transaction_pool) == 2)
