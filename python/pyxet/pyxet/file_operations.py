import os
import posixpath
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from fnmatch import fnmatch
from collections import namedtuple

if 'SPHINX_BUILD' not in os.environ:
    from .rpyxet import rpyxet

import fsspec

from . import XetFS, XetFSOpenFlags
from .url_parsing import parse_url
from .util import _path_split, _path_normalize, _path_join, \
  _path_dirname, _isdir, _get_fs_and_path, _rel_path, _are_same_fs, \
  _get_normalized_path

MAX_CONCURRENT_COPIES = threading.Semaphore(32)
CHUNK_SIZE = 16 * 1024 * 1024


def _validate_xet_copy(src_fs, src_path, dest_fs, dest_path):
    """
    Performs some basic early validation of a xet to avoid issues later.
    Does not catch all failure conditions, but catches some early enough to
    avoid doing a lot of unnecessary work, then fail.

    Raises an exception on failure, returns True on success
    """
    srcproto = src_fs.protocol
    destproto = dest_fs.protocol
    # if src is a xet, there must be a branch to copy from
    if srcproto == 'xet':
        src_fs.branch_info(src_path)

    if destproto == 'xet':
        # check dest branch exists
        # exists before we try to do any copying
        # An exception is that if this operation would create a branch
        if srcproto == 'xet':
            src_parse = parse_url(src_path, src_fs.domain)
            dest_parse = parse_url(dest_path, dest_fs.domain)
            if src_parse.path == '' and dest_parse.path == '':
                # this is a branch to branch copy
                return True

        dest_fs.branch_info(dest_path)


class CopyUnit:
    __slots__ = ['src_path', 'dest_path', 'dest_dir', 'size']

    def __init__(self, src_path, dest_path, dest_dir, size = None):
        self.src_path = src_path
        self.dest_path = dest_path
        self.dest_dir = dest_dir
        self.size = size

    def __repr__(self):
        return f"[CopyUnit: {self.src_path} to {self.dest_path} (dir = {self.dest_dir}), size = {self.size}]"

def _single_file_copy_impl(cp_action, src_fs, dest_fs, progress_reporter = None, buffer_size=CHUNK_SIZE):

    src_path = _path_normalize(src_fs, cp_action.src_path, strip_trailing_slash=True, keep_relative=False)
    dest_path = _path_normalize(dest_fs, cp_action.dest_path, strip_trailing_slash=True, keep_relative=False) 

    if progress_reporter is None:
        print(f"Copying {src_path} to {dest_path}")

    if src_fs.protocol == 'xet' and dest_fs.protocol == 'xet':
        ignore_size = True
        dest_fs.cp_file(src_path, dest_path)
        return
    else:
        ignore_size = False
    
    dest_is_xet = dest_fs.protocol == "xet"

    with MAX_CONCURRENT_COPIES:
        try:
            if cp_action.dest_dir is not None and not dest_is_xet: 
                dest_fs.makedirs(cp_action.dest_dir, exist_ok = True)

            if dest_is_xet:
                size_hint = cp_action.size

                if size_hint is None:
                    size_hint = src_fs.info(src_path).get('size', None)

                # Heuristic for now -- if the size of the source is larger than 50MB,
                # then make sure we have any shards for the destination that work.
                if size_hint is not None and size_hint >= 50000000:
                    dest_fs.add_deduplication_hints(dest_path)

            # Fasttrack for downloading a file to local
            if src_fs.protocol == "xet" and dest_fs.protocol == "file":
                with src_fs.open(src_path, "rb", flags=XetFSOpenFlags.FILE_FLAG_NO_BUFFERING) as source_file:
                    source_file.get(dest_path)
            else:
                with src_fs.open(src_path, "rb") as source_file:
                    with dest_fs.open(dest_path, "wb", auto_mkdir=True) as dest_file:
                        # Buffered copy in chunks
                        while True:
                            chunk = source_file.read(buffer_size)
                            if not chunk:
                                break
                            dest_file.write(chunk)

                            if progress_reporter:
                                progress_reporter.register_progress(None, len(chunk))

        except Exception as e:
            proto = src_fs.protocol
            print(f"Failed to copy {proto}://{src_path}: {e}")
            raise

        if progress_reporter:
            progress_reporter.register_progress(1, None)

        

        
def single_file_copy(src_fs, src_path, dest_fs, dest_path, size_hint = None):
    """
    Immediately performs a copy of a single file.

    Note this requires the destination to be inside a transaction.
    """
    
    # Our target is an existing directory i.e. we are getting cp src/some/path to dest/some/where
    # but dest/some/where exists as a directory
    # So we will need to make the dest dest/some/where/path
    if _isdir(dest_fs, dest_path): 
        # Split up the final component from source path and add it
        # to the destination.
        # We get final component from 'source' instead of the 
        # normalized 'src_path' because if 'source' ends with 
        # '/' or '\', the desired behavior is to copy what's 
        # under that directory, and to skip 'source' itself.
        _, final_source_component = _path_split(src_fs, src_path)
        file_dest_path = _path_join(dest_fs, dest_path, final_source_component)
        cp_unit = CopyUnit(src_path=src_path, dest_path=file_dest_path, dest_dir = dest_path, size = size_hint)
    else:
        dest_dir, _ = _path_split(dest_fs, dest_path)
        cp_unit = CopyUnit(src_path=src_path, dest_path=dest_path, dest_dir = dest_dir, size = size_hint)

    progress_reporter = rpyxet.PyProgressReporter(f"Copying {src_fs.protocol}://{src_path} to {dest_fs.protocol}://{dest_path}", 1, size_hint)
    _single_file_copy_impl(cp_unit, src_fs, dest_fs, progress_reporter)


def _build_cp_action_list_impl(src_fs, src_path, dest_fs, dest_path, recursive, progress_reporter):
    
    # This function has two parts.  First, we set a number of variables that then determine how 
    # the second section will behave.  

    dest_is_xet = dest_fs.protocol == "xet"

    # If both the source and the end is a xet, then we can copy directories as entire units. 
    cp_xet_to_xet = dest_is_xet and src_fs.protocol == "xet"

    # If the destination is specified as a directory, then we want to respect that, putting
    # things as needed within that directory or erring out if it exists but is not itself a 
    # directory.
    dest_specified_as_directory = dest_path.endswith("/") 
    dest_path = dest_path.rstrip("/")

    # Now set the following variables depending on src_path and dest_path, and what dest_path is
    # on the remote.
    file_filter_pattern = None  # If not none, only files matching this pattern will be copied.
    src_info = None  # Info about the source, if applicable.  Saves calls to info, which may be expensive. 
    src_is_directory = None     # True if the src is a directory.  This must be set after wildcards are detected.
    dest_dir = None   # The destination directory or branch containing the dest_path. 

    if '*' in src_path:
        # Handling wildcard cases.  
        
        # The file card matching works by recasting the operation as a directory -> directory copy, 
        # but with a possible filter pattern. 

        # validate
        # we only accept globs of the for blah/blah/blah/[glob]
        # i.e. the glob is only in the last component
        # src_root_dir should be blah/blah/blah here
        src_root_dir, end_component = _path_split(src_fs, src_path)

        if '*' in src_root_dir:
            raise ValueError(
                f"Invalid glob {src_path}. Wildcards can only appear in the last position")

        src_path = src_root_dir 
        src_is_directory = True # Copying the contents of the new source directory 
        file_filter_pattern = end_component if end_component != "*" else None

        dest_specified_as_directory = True

        dest_dir=dest_path  # Containing dir.
        dest_path=None  # In this case, the path information will be taken from the source file name.

    else:

        # Validate that the source path is specified correctly, and set src_is_directory. 
        if src_path.endswith("/"):
            src_path = src_path.rstrip("/")
            src_info = src_fs.info(src_path)
            src_is_directory = True
            if not src_info['type'] in ["directory", "branch"]: 
                raise ValueError(f"Source path {src_path} not an existing directory.")
        else:
            src_info = src_fs.info(src_path)
            src_is_directory = src_info['type'] in ["directory", "branch"]

        # Handling directories.  Make sure that the recursive flag is set. 
        if src_is_directory:
            if not recursive:
                print(f"{src_path} is a directory (not copied).")
                return


        # Now, determine the type of the destination: 
        try:
            if dest_fs.info(dest_path)["type"] in ["directory", "branch"]:
                dest_type = "directory"
            else: 
                dest_type = "file"
        except FileNotFoundError:
            dest_type = "nonexistant"
        

        if dest_type == "directory": 

            if src_is_directory:
                _, end_component = _path_split(src_fs, src_path)
                dest_dir = _path_join(dest_fs, dest_path, end_component)
                dest_path = None
            else:
                _, end_component = _path_split(src_fs, src_path)
                dest_path = _path_join(dest_fs, dest_path, end_component)
                dest_dir = _path_dirname(dest_fs, dest_path)

        elif dest_type == "file":
            if src_is_directory:
                # Dest exists, but it's a regular file.
                raise ValueError(
                    "Copy: source is a directory, but destination is not.")
            else:
                # dest_path is set correctly.
                dest_dir = _path_dirname(dest_fs, dest_path)

        elif dest_type == "nonexistant": 
            # When the destination doesn't exist
            if src_is_directory:
                dest_dir = dest_path
                dest_path = None
            else:
                # Slightly different behavior depending on whether the destination is specified as 
                # a directory or not.  If it is, then copy the source file into the dest path, otherwise 
                # copy the source to the dest path name. 
                if dest_specified_as_directory:
                    # xet cp dir/subdir/file dest/d1/  -> goes into dest/d1/file
                    dest_dir = dest_path
                    _, end_component = _path_split(src_fs, src_path)
                    dest_path = _path_join(dest_fs, dest_dir, end_component)
                else:
                    # xet cp dir/subdir/file dest/f1  -> goes into dest/f1
                    dest_dir = _path_dirname(dest_fs, dest_path)
        else:
            assert False


    # Now, we should have all the variables -- src_is_directory, src_path, dest_path, dest_dir, and file_filter_pattern 
    # set up correctly.  The easiest way to break this up is between the multiple file case (src_is_directory = True) and the 
    # single file case.
    if src_is_directory:

        # With the source a directory, we need to list out all the files to copy.  
        if recursive:
            # Handle the xet -> xet case
            if file_filter_pattern is None and cp_xet_to_xet:
                if progress_reporter:
                    progress_reporter.update_target(1, None)
                yield CopyUnit(src_path=src_path, dest_path=dest_dir, dest_dir=None, size=None)
                                 
            # If recursive, use find; this returns recursively.
            src_listing = src_fs.find(src_path, detail=True).items()

        else:
            # This is not recursive, so the src was specified as src_dir/<pattern>, e.g. src_dir/*.
            # In this case, use glob to list out all the files (glob is not recursive).
            pattern = _path_join(src_fs, src_path, file_filter_pattern if file_filter_pattern is not None else '*')
            src_listing = src_fs.glob(pattern, detail=True).items()


        for src_p, info in src_listing:
            
            if info['type'] == 'directory':
                # Find is already recursive, and glob is not used in the recursive case, so 
                # when this happens we can skip it. 
                continue

            # Get the relative path, so we can construct the full destination path
            rel_path = _rel_path(src_p, src_path)
            rel_path = _path_normalize(src_fs, rel_path, strip_trailing_slash=False, keep_relative=True)

            if file_filter_pattern is not None:
                if not fnmatch(rel_path, file_filter_pattern):
                    continue

            dest_path = _path_join(dest_fs, dest_dir, rel_path)
            base_dir = _path_dirname(dest_fs, dest_path)
            
            size = None if cp_xet_to_xet else info.get('size', 0)
            if progress_reporter:
                progress_reporter.update_target(1, size)
            yield CopyUnit(src_path=src_p, dest_path = dest_path, dest_dir = base_dir, size = size)

    else: # src_is_directory = False

        # In this case, we have just a single source file. 
        if cp_xet_to_xet:
            src_size = None
        elif src_info is not None:
            src_size = src_info.get('size', 0)
        else:
            src_size = src_fs.info(src_path).get('size', 0)

        # Do we copy this single file into the dest, or to the dest?
        if _isdir(dest_fs, dest_path):
            _, file_name = _path_split(src_fs, src_path)
            if progress_reporter:
                progress_reporter.update_target(1, src_size)
            yield CopyUnit(src_path=src_path, dest_path=os.path.join(dest_path, file_name), dest_dir=dest_path, size=src_size)
        else:
            dest_dir=_path_dirname(dest_fs, dest_path)
            if progress_reporter:
                progress_reporter.update_target(1, src_size)
            yield CopyUnit(src_path=src_path, dest_path=dest_path, dest_dir=dest_dir, size=src_size)




def build_cp_action_list(source, destination, recursive=False):
    """
    Builds a list of actions that need to be performed to do a copy operation.
    """

    src_fs, src_path = _get_fs_and_path(source, strip_trailing_slash=False)
    dest_fs, dest_path = _get_fs_and_path(destination, strip_trailing_slash=False)

    return list(_build_cp_action_list_impl(
        src_fs, src_path, dest_fs, dest_path, recursive, progress_reporter=None))
    

def perform_copy(source_list, destination, message = None, recursive=False):
    """
    Performs a copy operation. 
    """

    if not isinstance(source_list, list):
        source_list = [source_list]

    if len(source_list) == 0:
        raise ValueError("Empty source list")

    if message is None:
        message = f"copy {', '.join(source[:3])}... to {destination}" if not recursive else f"copy {', '.join(source[:3])}... to {destination} recursively"

    progress_reporter = rpyxet.PyProgressReporter(message, 0, 0)
    
    if not _are_same_fs(source_list):
        raise ValueError("Source URIs are not in the same filesystem")

    src_fs, src_path = _get_fs_and_path(source_list[0], strip_trailing_slash=False)
    dest_fs, dest_path = _get_fs_and_path(destination, strip_trailing_slash=False)
    
    destproto_is_xet = dest_fs.protocol == "xet"
    
    _validate_xet_copy(src_fs, src_path, dest_fs, dest_path)
    
    if destproto_is_xet:
        dest_fs.start_transaction(message)
    
    try:

        # Get the list of everything to copy.

        # Now, go through and do all the actual copying.
        futures = []
        opt_future = None
        with ThreadPoolExecutor() as executor:
            for source in source_list:
                src_path = _get_normalized_path(source, src_fs)
                for cp_action in _build_cp_action_list_impl(
                    src_fs, src_path, dest_fs, dest_path, recursive, progress_reporter):

                    futures.append(executor.submit(_single_file_copy_impl, cp_action,
                                src_fs, dest_fs, progress_reporter))

        for future in futures:
            future.result()

        if opt_future is not None:
            # Head scratch -- will this actually cancel it when it's running in rust?
            opt_future.cancel()

    finally:
        if destproto_is_xet:
            dest_fs.end_transaction()

        progress_reporter.finalize()
