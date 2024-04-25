from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial

from pyxet.util import _get_fs_and_path, _isdir, _rel_path, _path_join, _path_split, _path_dirname, _is_illegal_subdirectory_file_name
from pyxet.file_operations import _single_file_copy_impl, CopyUnit

XET_MTIME_FORMAT = '%Y-%m-%dT%H:%M:%S%z'


class SyncCommand:

    def __init__(self, source, destination, use_mtime, message, dryrun, update_size):
        self._message = message
        self._dryrun = dryrun
        self._src_fs, self._src_proto, self._src_root = _get_normalized_fs_protocol_and_path(source)
        self._dest_fs, self._dest_proto, self._dest_root = _get_normalized_fs_protocol_and_path(destination)
        self._use_mtime = use_mtime
        self._update_size = update_size
        if use_mtime:
            self._cmp = MTimeSyncComparator(self._src_proto, self._dest_proto)
        else:
            self._cmp = SizeOnlySyncComparator()

    def validate(self):
        """
        Performs early validation for the source and destination paths of
        a xet sync. Doesn't catch all failure conditions, but catches many
        of the easy validations.

        Raises exceptions on failure
        """

        if self._dest_proto != 'xet':
            raise ValueError(f"Unsupported destination protocol: {self._dest_proto}, only xet:// targets are supported")
        if self._src_proto == 'xet':
            raise ValueError(f"Unsupported source protocol: {self._src_proto}, only non-xet sources are supported")

        # check that the destination specifies an existing branch
        # TODO: we may want to be able to sync remote location to a new branch?
        self._dest_fs.branch_info(self._dest_root)

        # s3 needs a bucket
        if self._src_proto == 's3' and (self._src_root == '/' or self._src_root == ''):
            raise ValueError(f"S3 source needs a specified bucket")

        # wildcards not supported
        if '*' in self._src_root or '*' in self._dest_root:
            raise ValueError(f"Wildcards not supported in paths")

    def run(self):
        """
        Runs this Sync command, returning SyncStats containing the number of files copied
        during the sync.
        """
        sync_stats = SyncStats()

        srcpath_is_dir = _isdir(self._src_fs, self._src_root)
        if not self._dryrun:
            self._dest_fs.start_transaction(self._message)
        with ThreadPoolExecutor() as executor:
            futures = []
            # if src is a single file, we always use sync_with_info
            if self._use_mtime or srcpath_is_dir == False:
                self._sync_with_info(executor, futures, self._src_root, self._dest_root)
            else:
                self._sync_with_ls(executor, futures, self._src_root, self._dest_root)

            # Waiting for all copy jobs to complete
            for future in futures:
                try:
                    was_copied = future.result()
                    if was_copied:
                        sync_stats.copied += 1
                    else:
                        sync_stats.ignored += 1
                except Exception as e:
                    print(f"Error: {e}")
                    sync_stats.failed += 1

        if not self._dryrun:
            self._dest_fs.end_transaction()

        return sync_stats

    def _sync_with_ls(self, executor, futures, src_path, dest_path):
        """
        Sync the src_path to the dest_path using ls calls on both paths and comparing the
        two.

        Note that ls on xet-fs doesn't return an mtime and thus, will not be able to compare
        on that field.
        """
        # This only syncs folders. single files should always go to sync_with_info
        try:
            dest_files = self._dest_fs.find(dest_path, detail=True)
        except RuntimeError:
            dest_files = {}
        total_size = 0
        for abs_path, src_info in self._src_fs.find(src_path, detail=True).items():
            relpath = _rel_path(abs_path, src_path)

            if _is_illegal_subdirectory_file_name(relpath):
                print(f"{abs_path} is an invalid file (not copied).")
                continue

            dest_for_this_path = _path_join(self._dest_fs, dest_path, relpath)
            dest_info = dest_files.get(dest_for_this_path)

            partial_func = partial(self._sync_file_task, abs_path, src_info, dest_for_this_path, dest_info)
            futures.append(executor.submit(partial_func))
            total_size += src_info.get('size', 0)

        if self._update_size:
            self._update_remote_size(total_size)

    def _sync_with_info(self, executor, futures, src_path, dest_path):
        """
        Sync the src_path to the dest_path by calling `info` on the destination for files
        found in the source. This is much slower than
        """
        total_size = 0
        if not _isdir(self._src_fs, src_path):
            src_info = self._src_fs.info(src_path)
            abs_path = src_info['name']
            # get the last component name. This is the filename
            fname = _path_split(self._src_fs, abs_path)[-1]
            # Tack it on to the end of the dest path to make the output name
            dest_for_this_path = _path_join(self._dest_fs, dest_path, fname)
            partial_func = partial(self._sync_with_mtime_task, abs_path, dest_for_this_path, src_info)
            futures.append(executor.submit(partial_func))
            total_size += src_info.get('size', 0)
        else:
            for abs_path, src_info in self._src_fs.find(src_path, detail=True).items():
                relpath = _rel_path(abs_path, src_path)

                if _is_illegal_subdirectory_file_name(relpath):
                    print(f"{abs_path} is an invalid file (not copied).")
                    continue

                dest_for_this_path = _path_join(self._dest_fs, dest_path, relpath)
                if src_info['type'] != 'directory':
                    partial_func = partial(self._sync_with_mtime_task, abs_path, dest_for_this_path, src_info)
                    futures.append(executor.submit(partial_func))
                    total_size += src_info.get('size', 0)

        if self._update_size:
            self._update_remote_size(total_size)

    def _sync_with_mtime_task(self, src_path, dest_path, src_info):
        """
        Fetch info for the dest_path from remote and use that to sync the file
        """
        try:
            dest_info = self._dest_fs.info(dest_path)
        except FileNotFoundError:
            dest_info = None
        return self._sync_file_task(src_path, src_info, dest_path, dest_info)

    def _sync_file_task(self, src_path, src_info, dest_path, dest_info):
        """
        Task to sync the src to the dest using self's SyncComparator to determine if the files
        should be copied.

        Will return whether the file was copied or not.
        """
        if dest_info is not None and src_info['type'] != dest_info['type']:
            print(f"Copy failed: {src_path} is a {src_info['type']}, {dest_path} is a {dest_info['type']}")
            raise ValueError(f"{src_path} and {dest_path} are not the same type of entry")

        size = src_info.get('size', None)
        if dest_info is None or self._cmp.should_sync(src_info, dest_info):
            if not self._dryrun:
                dest_dir = _path_dirname(self._dest_fs, dest_path)
                cp_copy = CopyUnit(src_path=src_path, dest_path=dest_path, dest_dir = dest_dir, size = size)
                _single_file_copy_impl(cp_copy, self._src_fs, self._dest_fs)
            else:
                print(f"Copying {src_path} to {dest_path}")
            return True
        # ignored
        return False

    def _update_remote_size(self, size):
        """
        Update Xetea with the new bucket size.
        """
        if self._dest_proto == 'xet':
            print(f"Updating {self._dest_root} with the discovered bucket size: {size}")
            self._dest_fs.update_size(self._dest_root, size)
        else:
            print(f"Can't update bucket size since destination is protocol: {self._dest_proto}, not xet")


def _get_normalized_fs_protocol_and_path(uri):
    """
    Take the fsspec path (<proto>://<path>) and return the
    FS object, protocol, and path, removing any trailing slash
    on the path (unless the path indicates the root dir)
    """
    fs, path = _get_fs_and_path(uri)
    # normalize trailing '/' by just removing them unless the path
    # is exactly just '/'
    if path != '/':
        path = path.rstrip('/')
    return fs, fs.protocol, path


def _get_last_modified(protocol: str, info: dict) -> float:
    if protocol == 'xet':
        mod_time = info['last_modified']  # str
        mod_time = datetime.strptime(mod_time, XET_MTIME_FORMAT).timestamp()
    elif protocol == 's3':
        mod_time = info['LastModified']  # datetime
        mod_time = mod_time.timestamp()
    elif protocol == 'file':
        mod_time = info['mtime']  # float
    else:
        print(f"WARN: protocol: {protocol} doesn't have a modification time, only comparing size")
        mod_time = None

    return mod_time


class SyncComparator(ABC):
    @abstractmethod
    def should_sync(self, src_info, dest_info):
        pass


class SizeOnlySyncComparator(SyncComparator):
    """
    Compare info only by size. If the sizes are different,
    then we should sync.
    """

    def should_sync(self, src_info, dest_info):
        return src_info['size'] != dest_info['size']


class MTimeSyncComparator(SyncComparator):
    """
    Compare info by size and mtime.
    We should sync only if the size's match and
    the mtime for the source is larger than the
    mtime of the destination.
    """
    def __init__(self, src_proto, dest_proto):
        self._src_proto = src_proto
        self._dest_proto = dest_proto

    def should_sync(self, src_info, dest_info):
        if src_info['size'] != dest_info['size']:
            return True

        src_mtime = _get_last_modified(self._src_proto, src_info)
        dest_mtime = _get_last_modified(self._dest_proto, dest_info)
        return src_mtime is not None and dest_mtime is not None \
            and src_mtime > dest_mtime


class SyncStats:
    copied = 0
    ignored = 0
    failed = 0
