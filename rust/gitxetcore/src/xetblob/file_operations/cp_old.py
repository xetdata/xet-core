
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


