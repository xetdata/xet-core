import os

import pyxet
from pyxet.sync import _get_normalized_fs_protocol_and_path, SyncCommand

from utils import CONSTANTS, require_s3_creds


def test_get_normalized_fs_protocol_and_path(monkeypatch):
    def validate(uri, exp_proto, exp_path):
        fs, proto, path = _get_normalized_fs_protocol_and_path(uri)
        assert fs is not None
        assert proto == exp_proto
        assert fs.protocol == proto
        assert path == exp_path

    # TODO: Fix check for windows
    # validate('/foo/bar/', 'file', os.path.normpath('/foo/bar'))
    # validate('/', 'file', os.path.normpath('/'))
    # validate('some/path', 'file', os.path.normpath(os.getcwd() + '/some/path'))
    # validate('.', 'file', os.getcwd())

    # Don't connect to S3.
    monkeypatch.setattr('pyxet.util._should_load_aws_credentials', lambda: False)
    validate('s3://bucket/obj', 's3', 'bucket/obj')
    validate('s3://bucket', 's3', 'bucket')
    validate('s3://bucket/dir/', 's3', 'bucket/dir')

    validate('xet://user/repo/branch', 'xet', 'user/repo/branch')
    validate('xet://user/repo/branch/sub/dir/', 'xet', 'user/repo/branch/sub/dir')
    validate('xet://user/repo/', 'xet', 'user/repo')


def check_sync_validate(src, dst, is_valid):
    cmd = SyncCommand(src, dst, False, '', False, False)
    try:
        cmd.validate()
        assert is_valid
    except (ValueError, FileNotFoundError):
        assert not is_valid


def test_sync_command_validate_local():
    pyxet.login(CONSTANTS.TESTING_USERNAME, CONSTANTS.TESTING_TOKEN, email="a@a.com")

    check_sync_validate('.', f'xet://{CONSTANTS.TESTING_SYNCREPO}/main', True)
    check_sync_validate('xet://XetHub/grapp2/main', f'xet://{CONSTANTS.TESTING_SYNCREPO}/sync-branch/sync', False)
    check_sync_validate('.', f'xet://{CONSTANTS.TESTING_SYNCREPO}/nonexistent-branch', False)
    check_sync_validate('.', './other', False)
    check_sync_validate('.', f'xet://{CONSTANTS.TESTING_SYNCREPO}', False)
    check_sync_validate('.', 'xet://', False)
    check_sync_validate('./*', f'xet://{CONSTANTS.TESTING_SYNCREPO}/main', False)
    check_sync_validate('./*.py', f'xet://{CONSTANTS.TESTING_SYNCREPO}/main', False)
    check_sync_validate('.', f'xet://{CONSTANTS.TESTING_SYNCREPO}/main/foo*', False)


@require_s3_creds()
def test_sync_command_validate_s3():
    pyxet.login(CONSTANTS.TESTING_USERNAME, CONSTANTS.TESTING_TOKEN, email="a@a.com")

    check_sync_validate(f's3://{CONSTANTS.S3_BUCKET}', f'xet://{CONSTANTS.TESTING_SYNCREPO}/main', True)
    check_sync_validate(f's3://{CONSTANTS.S3_BUCKET}/nonexistent-path', f'xet://{CONSTANTS.TESTING_SYNCREPO}/main', False)
    check_sync_validate(f's3://', f'xet://{CONSTANTS.TESTING_SYNCREPO}/main', False)
    check_sync_validate(f's3://{CONSTANTS.S3_BUCKET}', f's3://{CONSTANTS.S3_BUCKET}/other', False)


def get_rand_name(prefix):
    import time
    ts = int(time.time())
    return f'{prefix}_{ts}'


@require_s3_creds()
def test_sync_command_s3():
    # TODO: fix for windows paths
    import sys
    if sys.platform.startswith('win'):
        return

    pyxet.login(CONSTANTS.TESTING_USERNAME, CONSTANTS.TESTING_TOKEN, email="a@a.com")
    fs = pyxet.XetFS()
    branch = get_rand_name('sync_test_s3')
    fs.make_branch(CONSTANTS.TESTING_SYNCREPO, 'main', branch)
    src_path = f'{CONSTANTS.TESTING_SYNCREPO}/{branch}'

    cmd = SyncCommand(f's3://{CONSTANTS.S3_BUCKET}/sync1', f'xet://{src_path}', False, 'test sync from s3', False, False)
    cmd.validate()
    stats = cmd.run()
    assert stats.ignored == 0
    assert stats.copied == 2
    assert stats.failed == 0

    files = fs.find(src_path, detail=True)
    assert len(files) == 3  # .gitattributes is also in repo
    assert files.get(f'{src_path}/js/main.d8604548.js')
    assert files.get(f'{src_path}/tmp-1')

    # no files should be copied
    stats = cmd.run()
    assert stats.ignored == 2
    assert stats.copied == 0
    assert stats.failed == 0

    fs.delete_branch(CONSTANTS.TESTING_SYNCREPO, branch)
