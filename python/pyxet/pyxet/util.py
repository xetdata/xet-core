import os
import posixpath
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import fsspec

from . import XetFS, XetFSOpenFlags
from .url_parsing import parse_url

MAX_CONCURRENT_COPIES = threading.Semaphore(32)
CHUNK_SIZE = 16 * 1024 * 1024

def _should_load_aws_credentials():
    """
    Determines if AWS credentials should be loaded for s3 API by checking if credentials are available
    :return: boolean, True if credentials are available, False
    """
    try:
        import boto3
        import botocore
    except:
        print("boto3 and botocore required for AWS S3 support")
        print("Please install them with 'pip install boto3 botocore'")
        sys.exit(1)
    client = boto3.client("sts")
    try:
        client.get_caller_identity()
    except botocore.exceptions.NoCredentialsError:
        return False
    return True


def _get_fs_and_path(uri, strip_trailing_slash = True):
    if uri.find('://') == -1:
        fs = fsspec.filesystem("file")
        return fs, _path_normalize(fs, uri, strip_trailing_slash=strip_trailing_slash)
    split = uri.split("://")
    if len(split) != 2:
        print(f"Invalid URL: {uri}", file=sys.stderr)
    if split[0] == 'xet':
        fs = XetFS()
    elif split[0] == 's3':
        load_aws_creds = _should_load_aws_credentials()
        fs = fsspec.filesystem('s3', anon=not load_aws_creds)
        # this is *really* annoying But the s3fs protocol has
        # protocol as a list ['s3','s3a']
        fs.protocol = 's3'
    else:
        fs = fsspec.filesystem(split[0])
    return fs, _path_normalize(fs, split[1], strip_trailing_slash=strip_trailing_slash)

def _get_normalized_path(uri, fs, strip_trailing_slash = True):
    if uri.find('://') == -1:
        return _path_normalize(fs, uri, strip_trailing_slash=strip_trailing_slash)
    split = uri.split("://")
    if len(split) != 2:
        raise ValueError(f"Invalid URL: {uri}")
    return _path_normalize(fs, split[1], strip_trailing_slash=strip_trailing_slash)

def _isdir(fs, path):
    if fs.protocol == 'xet':
        return fs.isdir_or_branch(path)
    else:
        return fs.isdir(path)

def _get_fs_string(uri):
    n = uri.find('://')
    if n == -1:
        return "local"
    else:
        return uri[:n]

def _are_same_fs(uris):
    return len(set(map(lambda u: _get_fs_string(u), uris))) == 1

def _rel_path(s, start):
    """
    Get the relative path of 's' from 'start'
    Ex:
    ```
       _rel_path("a/b/c.txt", "a/b") => "c.txt"
    ```
    """
    return os.path.relpath(s, start)


# split path into dirname and basename
def _path_split(fs, path):
    if fs.protocol == 'file':
        return os.path.split(path)
    else:
        return path.rsplit('/', 1)


def _path_join(fs, path, *paths):
    if fs.protocol == 'file':
        return os.path.join(path, *paths)
    else:
        return '/'.join([path] + [p.strip('/') for p in paths if len(p) != 0])


def _path_dirname(fs, path):
    if fs.protocol == 'file':
        return os.path.dirname(path)
    else:
        return '/'.join(path.split('/')[:-1])
    
def _path_normalize(fs, path, strip_trailing_slash = True, keep_relative = False):
    if fs.protocol == 'file':
        if not keep_relative:
            path = os.path.abspath(path)
        if os.sep != posixpath.sep:
            path = path.replace(os.sep, posixpath.sep)
        return path
    elif path != '/' and strip_trailing_slash:
        return path.rstrip('/')
    else:
        return path

def _is_illegal_subdirectory_file_name(path):
    return path == '.' or path == '' or path == '..'