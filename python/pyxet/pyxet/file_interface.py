import io


class XetFile:
    """
    A handle to a file in a Xet repo.  
    """

    def __init__(
            self,
            handle,
            write_transaction=None,
    ):
        # Get pyxethandle  from path if None.
        self.handle = handle
        self.write_transaction = write_transaction
        self._do_not_write = False

    @property
    def closed(self):
        return self.handle.is_closed()

    def close(self):
        if self.write_transaction:
            self.write_transaction.close()
            self.write_transaction = None
        if not self.closed:
            return self.handle.close()

    def isatty(self):
        return False

    def flush(self):
        pass

    def readable(self):
        return self.handle.readable()

    def seekable(self):
        return self.handle.seekable()

    def writable(self):
        return self.handle.writable()

    def readline(self, size=-1):
        if not self.readable():
            raise RuntimeError("Read not supported")
        if size is None:
            return self.handle.readline(-1)
        if isinstance(size, int):
            return self.handle.readline(size)
        raise TypeError("size must be an integer")

    def readlines(self, hint=-1):
        if not self.readable():
            raise RuntimeError("Read not supported")
        if hint is None:
            return self.handle.readlines(-1)

        if isinstance(hint, int):
            return self.handle.readlines(hint)

        raise TypeError("hint must be an integer")

    def seek(self, offset, whence=io.SEEK_SET):
        if not self.seekable():
            raise RuntimeError("Seek not supported")
        if whence not in (io.SEEK_SET, io.SEEK_CUR, io.SEEK_END):
            raise ValueError("Unexpected value for whence")
        if not isinstance(offset, int):
            raise TypeError("Unexpected type for offset")

        return self.handle.seek(offset, whence)

    def tell(self):
        if not self.seekable():
            raise RuntimeError("Tell not supported")
        return self.handle.tell()

    def read(self, size=-1):
        if not self.readable():
            raise RuntimeError("Read not supported")
        if not isinstance(size, int):
            raise TypeError("Unexpected type for size")
        return self.handle.read(size)

    def readall(self):
        if not self.readable():
            raise RuntimeError("Read not supported")
        return self.handle.readall()

    def readinto(self, b):
        if not self.readable():
            raise RuntimeError("Read not supported")
        return self.handle.readinto(b)

    def readinto1(self, b):
        if not self.readable():
            raise RuntimeError("Read not supported")
        return self.handle.readinto1(b)

    def write(self, data):
        if not self.writable():
            raise ValueError("File not in write mode")
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if self._do_not_write:
            return
        if isinstance(data, str):
            data = data.encode('utf-8')
        self.handle.write(data)

    def get(self, path):
        self.handle.get(path)

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __iter__(self):
        while True:
            yield self.readline()

    def _fake_writes(self):
        """
        Internal method to flag that all writes to this file
        are faked and will instead silently succeed
        """
        self._do_not_write = True
        return self
