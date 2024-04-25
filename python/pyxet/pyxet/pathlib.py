import contextlib
import io
import re
from pathlib import Path as PathlibPath

import pyxet
from .url_parsing import get_url_info


class Path:
    def __new__(cls, *args):
        try:
            return XetPath(*args)
        except ValueError:
            return PathlibPath(*args)


class XetPath:

    def __init__(self, uri: str):
        info = get_url_info(uri)

        self.uri = info.full_url()

        self._accessor = pyxet.XetFS(info)
        self.user = info.user
        self.repo = info.repo_url()
        self.branch = info.branch
        self.repo_url = info.repo_url()
        self.path = "/" if info.path is None else info.path

    @property
    def name(self):
        split = self.path.split('/')
        if len(split) == 0:
            return ''
        return split[-1]

    def __str__(self):
        return self.path

    def __repr__(self):
        return f"XetPath({self.path})"

    def read_text(self, encoding=None, errors=None):
        encoding = io.text_encoding(encoding)
        with self._accessor.open(self.path, mode='r', encoding=encoding, errors=errors) as f:
            return f.read()

    def read_bytes(self):
        with contextlib.suppress(OSError):
            info = self._accessor.info(self.uri)
            if info['type'] == 'directory':
                raise IsADirectoryError(f"{self.path} is a directory")
            elif info['type'] == 'file':
                with self._accessor.open(self.path, mode='rb') as f:
                    return f.read()
                raise IsADirectoryError()
        raise FileNotFoundError(f"No such file or directory: '{self.path}'")

    # TODO important

    def _get_commit_message(self, data):
        data_content = data[:50] + '...' if len(data) > 50 else data
        return f"write text to {self.path}: {data_content}"

    def write_text(self, data, encoding=None, errors=None, newline=None, commit_message=None):
        if not isinstance(data, str):
            raise TypeError('data must be str, not %s' %
                            data.__class__.__name__)
        if commit_message is None:
            commit_message = self._get_commit_message(data)
        encoding = io.text_encoding(encoding)
        with self._accessor.commit(commit_message=commit_message):
            with self._accessor.open(self.path, mode='w',
                                     encoding=encoding,
                                     errors=errors,
                                     newline=newline) as f:
                f.write(data)

    def write_bytes(self, data: bytes, commit_message=None):
        view = memoryview(data)
        if commit_message is None:
            commit_message = self._get_commit_message(data)
        with self._accessor.commit(commit_message=commit_message):
            with self._accessor.open(self.path, mode='wb') as f:
                return f.write(view)

    # TODO
    def stat(self):
        """
        return os.stat_result with the following attributes:
        st_mode, st_ino, st_dev, st_nlink, st_uid, st_gid, st_size, st_atime, st_mtime, st_ctime
        """
        raise NotImplementedError()

    # TODO - low priority
    def replace(self, target: str):
        """
        Rename this path to the target path, overwriting if that path exists.

        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.

        Returns the new Path instance pointing to the target path.
        """
        raise NotImplementedError()

    # TODO - high priority
    def unlink(self):
        """
        Remove this file or link.
        If the path is a directory, use rmdir() instead.
        """
        raise NotImplementedError()

    def glob(self, pattern: str):
        for path in self._accessor.glob(pattern):
            yield XetPath('/'.join([self.repo, self.branch, path]))

    def exists(self):
        with contextlib.suppress(OSError):
            self._accessor.info(self.uri)
            return True
        return False

    def home(self):
        return XetPath(self.repo)

    # TODO - change if we deal with relative paths
    def absolute(self):
        return self

    # TODO - change if we deal with relative paths
    def is_absolute(self):
        return True

    def is_dir(self):
        with contextlib.suppress(OSError):
            return self._accessor.info(self.path)['type'] == 'directory'
        return False

    def is_file(self):
        with contextlib.suppress(OSError):
            return self._accessor.info(self.path)['type'] == 'file'

    # TODO - low priority
    def is_fifo(self):
        """
        Whether this path is a FIFO.
        """
        raise NotImplementedError

    def is_mount(self):
        raise NotImplementedError()

    def iterdir(self):
        for path in self.glob('*'):
            yield XetPath(path)

    def joinpath(self, path: str):
        return XetPath('/'.join([self.uri, path]))

    # TODO - low priority
    def lstat(self):
        raise NotImplementedError()

    # TODO
    def samefile(self, path: str):
        """
        Probably needed to compare local and cloud versions of the same file
        @param path:
        @return:
        """
        raise NotImplementedError()

    def match(self, pattern: str):
        return re.match(pattern, self.path)

    # TODO - important
    def mkdir(self, mode: int, parents: bool, exist_ok: bool):
        raise NotImplementedError()

    # TODO - important
    def rmdir(self):
        raise NotImplementedError()

    def with_name(self, name: str):
        return self.parent().joinpath(name)

    def with_suffix(self, suffix: str):
        raise NotImplementedError()

    def with_stem(self, stem: str):
        raise NotImplementedError()

    @property
    def parent(self):
        split = self.path.split('/')
        if len(split) == 0 or split == ['']:
            return XetPath(self.repo_uri)
        return XetPath('/'.join([self.repo_uri, split[:-1]]))

    # TODO unclear if relevant
    def parents(self):
        raise NotImplementedError()

    # TODO - low priority
    def touch(self):
        raise NotImplementedError()

    # TODO unclear if relevant
    @property
    def root(self):
        return ""
