import json
import sys
from urllib.parse import urlparse
from enum import IntEnum

import fsspec
import os

from .commit_transaction import MultiCommitTransaction
from .file_interface import XetFile
from .url_parsing import parse_url, XetPathInfo

if 'SPHINX_BUILD' not in os.environ:
    from .rpyxet import rpyxet
    _manager = rpyxet.PyRepoManager()

def login(user, token, email=None, host=None):
    """
    Sets the active login credentials used to authenticate against Xethub.
    """
    _manager.override_login_config(user, token, email, host)


def open(file_url, mode="rb", **kwargs):
    """
    Open the file at the specific Xet file URL
    of the form `xet://<repo_user>/<repo_name>/<branch>/<path-to-file>`::

        f = pyxet.open('xet://XetHub/Flickr30k/main/results.csv')
    """

    fs = XetFS()
    return fs._open(file_url, mode=mode, **kwargs)

class XetFSOpenFlags(IntEnum):
    FILE_FLAG_NO_BUFFERING = 0x20000000

class XetFS(fsspec.spec.AbstractFileSystem):
    protocol = "xet"  # This allows pandas, etc. to implement "xet://"

    cachable = True
    sep = "/"
    async_impl = False
    root_marker = "/"

    def __init__(self, domain=None, **storage_options):
        """
        Opens the repository at `repo_url` as an fsspec file system handle,
        providing read-only operations such as ls, glob, and open.

        User and token are needed for private repositories and they
        can be set with `pyxet.login`.

        Examples::
        
            import pyxet
            fs = pyxet.XetFS()

            # List files.
            fs.ls('XetHub/Flickr30k/main')

            # Read the first 5 lines of a file
            b = fs.open('XetHub/Flickr30k/main/results.csv').read()

        the Xet repository endpoint can be set with the 'domain' argument
        or the XET_ENDPOINT environment variable. The default domain is
        xethub.com if unspecified
        """
        import os
        if 'XET_ENDPOINT' in os.environ:
            domain = os.environ['XET_ENDPOINT']
        if domain is None:
            domain = 'xethub.com'
        self.domain = domain
        self.intrans = False
        self._transaction = None

        # Init the base class.
        super().__init__()

    @classmethod
    def _strip_protocol(cls, path):
        """Turn path from fully-qualified to file-system-specific
        May require FS-specific handling, e.g., for relative paths or links.
        """

        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]

        if path.startswith('xet://'):
            protostripped = path[5:]
        elif path.startswith('https://'):
            protostripped = path[8:]
        else:
            protostripped = path
        return protostripped.lstrip('/')

    def get_username(self):
        """
        Returns the inferred username for the domain
        """
        return _manager.get_inferred_username(self.domain)

    def unstrip_protocol(self, name):
        """Format FS-specific path to generic, including protocol"""
        return 'xet://' + name.lstrip('/')

    @staticmethod
    def _get_kwargs_from_urls(path):
        """If kwargs can be encoded in the paths, extract them here
        This should happen before instantiation of the class; incoming paths
        then should be amended to strip the options in methods.
        Examples may look like an sftp path "sftp://user@host:/my/path", where
        the user and host should become kwargs and later get stripped.
        """
        return {}

    def isdir(self, path):
        """Is this entry directory-like?"""
        return self.isdir_or_branch(path)

    def isdir_or_branch(self, path):
        """Is this entry directory-like?"""
        try:
            t = self.info(path)["type"]
            return t == "directory" or t == "branch"
        except OSError:
            return False

    def branch_info(self, url):
        """
        Returns information about a branch `user/repo/branch` 
        or `xet://user/repo/branch`
        """
        # try to parse this as a URL
        # and if not try to parse it as a path
        if isinstance(url, XetPathInfo):
            url_path = url
        else:
            url_path = parse_url(url, self.domain)
        if url_path.branch == '':
            raise ValueError("Incomplete path: Expecting xet://user/repo/branch")

        parse = urlparse(url_path.remote)
        path = parse.path
        components = list(filter(None, path.lstrip('/').rstrip('/').split('/')))
        if len(components) < 2:
            raise ValueError("Incomplete path: Expecting xet://user/repo/branch")
        prefix = '/'.join(components[:2]) + '/' + url_path.branch
        attr = _manager.stat(url_path.remote, url_path.branch, "")
        if attr is None:
            raise FileNotFoundError(
                f"Branch or repo not found {url}, remote = {url_path.remote}, branch = {url_path.branch}")
        return {"name": prefix + '/' + url_path.path,
                "size": attr.size,
                "type": attr.ftype}

    def branch_exists(self, url):
        try:
            self.branch_info(url)
            return True
        except Exception as e:
            return False

    def info(self, url):
        """
        Returns information about a path `user/repo/branch/[path]` 
        or `xet://user/repo/branch/[path]`
        """
        url_path = parse_url(url, self.domain)
        if url_path.branch == '':
            raise ValueError("Incomplete path: Expecting xet://user/repo/branch/[path]")

        parse = urlparse(url_path.remote)
        path = parse.path
        components = path.lstrip('/').rstrip('/').split('/')
        if len(components) < 2:
            raise ValueError("URL not in recognized format.")
        prefix = '/'.join(components[:2]) + '/' + url_path.branch
        attr = _manager.stat(url_path.remote, url_path.branch, url_path.path)
        if attr is None:
            raise FileNotFoundError(f"File not found {url}")
        return {"name": prefix + '/' + url_path.path,
                "size": attr.size,
                "type": attr.ftype,
                "last_modified": None if len(attr.last_modified) == 0 else attr.last_modified}

    def make_repo(self, dest_path, private=False, **kwargs):
        dest = parse_url(dest_path, self.domain)
        if dest.path != '':
            raise ValueError("Expecting xet://user/repo for destination")
        if self.is_repo(dest_path):
            raise ValueError(f"{dest_path} already exists")

        domain = self.domain
        domain_split = domain.split('://')
        scheme = 'https'
        if len(domain_split) == 2:
            scheme = domain_split[0]
            domain = domain_split[1]

        # dest_path is of the form xet://user/repo
        split = dest_path.split('://')[-1].split('/')
        assert (len(split) == 2)
        owner, repo = split[0], split[1]

        query = json.dumps({'name': repo, 'owner': owner, 'private': private})
        ret = json.loads(bytes(_manager.api_query(f"{scheme}://{domain}", "", "post", query)))
        return ret

    def fork_repo(self, origin_path, dest_path, **kwargs):
        origin = parse_url(origin_path, self.domain)
        dest = parse_url(dest_path, self.domain)
        if origin.path != '':
            raise ValueError("Expecting xet://user/repo for fork origin")
        if dest.path != '':
            raise ValueError("Expecting xet://user/repo for destination")
        if not self.is_repo(origin_path):
            raise ValueError(f"{origin_path} is not a repo")
        if self.is_repo(dest_path):
            raise ValueError(f"{dest_path} already exists")
        user_and_repo = list(filter(None, dest_path.split('://')[-1].split('/')))
        if len(user_and_repo) != 2:
            raise ValueError("Expecting xet://user/repo for destination")
        # validate username
        user, new_repo = user_and_repo[0], user_and_repo[1]
        real_username = self.get_username()
        if user != real_username:
            raise ValueError(f"Cannot only create repository at xet://{real_username} and not xet://{user}")

        query = json.dumps({'name': new_repo})
        ret = json.loads(bytes(_manager.api_query(origin.remote, "forks", "post", query)))
        return ret

    def duplicate_repo(self, origin_path, dest_path, **kwargs):
        origin = parse_url(origin_path, self.domain)
        dest = parse_url(dest_path, self.domain)
        if origin.path != '':
            raise ValueError("Expecting xet://user/repo for fork origin")
        if dest.path != '':
            raise ValueError("Expecting xet://user/repo for destination")
        if not self.is_repo(origin_path):
            raise ValueError(f"{origin_path} is not a repo")
        if self.is_repo(dest_path):
            raise ValueError(f"{dest_path} already exists")
        user_and_repo = list(filter(None, dest_path.split('://')[-1].split('/')))
        if len(user_and_repo) != 2:
            raise ValueError("Expecting xet://user/repo for destination")
        # validate username
        user, new_repo = user_and_repo[0], user_and_repo[1]
        real_username = self.get_username()
        if user != real_username:
            raise ValueError(f"Cannot only create repository at xet://{real_username} and not xet://{user}")

        ret = json.loads(bytes(_manager.api_query(origin.remote, "duplicate", "post", "")))
        if 'full_name' not in ret:
            raise RuntimeError("Duplication failed")
        src_name = 'xet://' + ret['full_name']
        if src_name == dest_path:
            return ret
        return self.rename_repo(src_name, dest_path)

    def rename_repo(self, origin_path, dest_path, **kwargs):
        origin = parse_url(origin_path, self.domain)
        dest = parse_url(dest_path, self.domain)
        if origin.path != '':
            raise ValueError("Expecting xet://user/repo for fork origin")
        if dest.path != '':
            raise ValueError("Expecting xet://user/repo for destination")
        if not self.is_repo(origin_path):
            raise ValueError(f"{origin_path} is not a repo")
        if self.is_repo(dest_path):
            raise ValueError(f"{dest_path} already exists")

        user_and_repo = list(filter(None, origin_path.split('://')[-1].split('/')))
        if len(user_and_repo) != 2:
            raise ValueError("Expecting xet://user/repo for destination")
        old_user = user_and_repo[0]

        user_and_repo = list(filter(None, dest_path.split('://')[-1].split('/')))
        if len(user_and_repo) != 2:
            raise ValueError("Expecting xet://user/repo for destination")
        new_user, new_repo = user_and_repo[0], user_and_repo[1]

        if old_user != new_user:
            raise ValueError("Username must be the same between source and destination")

        query = json.dumps({'name': new_repo})
        ret = json.loads(bytes(_manager.api_query(origin.remote, "", "patch", query)))
        return ret

    def set_repo_attr(self, origin_path, attrkey, attrvalue, **kwargs):
        origin = parse_url(origin_path, self.domain)
        if origin.path != '':
            raise ValueError("Expecting xet://user/repo for fork origin")
        if not self.is_repo(origin_path):
            raise ValueError(f"{origin_path} is not a repo")

        query = json.dumps({attrkey: attrvalue})
        ret = json.loads(bytes(_manager.api_query(origin.remote, "", "patch", query)))
        return ret

    def list_repos(self, raw=False, **kwargs):
        """
        Lists the repos available for a path of the form `user` or `xet://user`
        """
        domain = self.domain
        domain_split = domain.split('://')
        scheme = 'https'
        if len(domain_split) == 2:
            scheme = domain_split[0]
            domain = domain_split[1]

        res = json.loads(bytes(_manager.api_query(f"{scheme}://{domain}", "", "get", "")))
        if raw:
            return res
        else:
            return [{'name': f['full_name'],
                     'permissions': f['permissions']} for f in res]

    def list_branches(self, path, raw=False, **kwargs):
        """
        Lists the branches for a path of the form `user/repo` or `xet://user/repo`
        """
        url_path = parse_url(path, self.domain)
        if url_path.remote == '':
            raise ValueError("Incomplete path: Expecting xet://user/repo")
        if url_path.branch != '':
            raise ValueError('Too many path components for a repo')

        res = json.loads(bytes(_manager.api_query(url_path.remote, "branches", "get", "")))
        if raw:
            return res
        else:
            return [{'name': r['name'], 'type': 'branch'} for r in res]

    def update_size(self, path, bucket_size):
        """
        Calls Xetea to update the size of a synchronized S3 bucket for the repo.
        """
        url_path = parse_url(path, self.domain)
        if url_path.remote == '':
            raise ValueError("Incomplete path: Expecting xet://user/repo/branch")
        if url_path.branch == '':
            raise ValueError("No branch in path")
        body = json.dumps({
            'size': bucket_size,
            'branch': url_path.branch
        })
        _manager.api_query(url_path.remote, "remote_size", "post", body)

    def ls(self, path, detail=True, **kwargs):
        """List objects at path.
        This should include subdirectories and files at that location. The
        difference between a file and a directory must be clear when details
        are requested.
        The specific keys, or perhaps a FileInfo class, or similar, is TBD,
        but must be consistent across implementations.
        Must include:

        - full path to the entry (without protocol)
        - size of the entry, in bytes. If the value cannot be determined, will
          be ``None``.
        - type of entry, "file", "directory" or other

        Additional information
        may be present, appropriate to the file-system, e.g., generation,
        checksum, etc.
        May use refresh=True|False to allow use of self._ls_from_cache to
        common where listing may be expensive.

        Parameters:

            path: str
            detail: bool
                if True, gives a list of dictionaries, where each is the same as
                the result of ``info(path)``. If False, gives a list of paths
                (str).
            kwargs: may have additional backend-specific options, such as version
                information

        Returns:
            List of strings if detail is False, or list of directory information
            dicts if detail is True.  These dicts would have: name (full path in the FS), 
            size (in bytes), type (file, directory, or something else) and other FS-specific keys.
        """
        # list user names
        if path == '':
            # if there are no paths. We list all the unique users
            # list_repos return username/repo so we split the name and 
            # unique the 1st component
            names = set([f['name'].split('/')[0] for f in self.list_repos()])
            return [{'name': n, 'type': 'user'} for n in names]
        path = path.rstrip('/')
        if len(path.split('/')) == 1:
            # if there exactly 1 component in the path it has to be [username]
            # list_repos return username/repo so we split the name and
            # and match every repo which username == path
            names = [f['name'] for f in self.list_repos() if f['name'].split('/')[0] == path]
            return [{'name': n, 'type': 'repo'} for n in names]

        url_path = parse_url(path, self.domain)

        if url_path.branch == '':
            branches = self.list_branches(path)
            return [{'name': path + '/' + n['name'], 'type': 'branch'} for n in branches]

        parse = urlparse(url_path.remote)
        path = parse.path
        components = path.lstrip('/').rstrip('/').split('/')
        if len(components) < 2:
            raise ValueError('Incomplete path. must be of the form user/repo/[branch]/[path]')
        prefix = '/'.join(components[:2]) + '/' + url_path.branch

        files, file_info = _manager.listdir(url_path.remote,
                                            url_path.branch,
                                            url_path.path)

        if detail:
            return [{"name": prefix + '/' + fname,
                     "size": finfo.size,
                     "type": finfo.ftype}
                    for fname, finfo in zip(files, file_info)]
        else:
            return files

    def _open(
            self,
            path,
            mode="rb",
            **kwargs,
    ):
        """
        Return raw bytes-mode file-like from the file-system.

        Reads can be performed from any where, but writes must be performed
        within the context of a transaction which must be scoped to within a
        single repository branch.
        """

        url_path = parse_url(path, self.domain)

        transaction = getattr(self, "_transaction", None)

        if transaction is None and not mode.startswith('r'):
            raise RuntimeError(
                "Write access to files is only allowed within a commit transaction.")

        if not mode.startswith('r'):
            # this is a write
            if self._transaction is None:
                # with no transaction
                raise RuntimeError("Write only allowed in the context of a commit transaction."
                                   "Use `with fs.transaction(repo_and_branch, [commit_message]):` to enable write access.")

        if mode.startswith('r'):
            repo_handle = _manager.get_repo(url_path.remote)
            branch = url_path.branch
            if "flags" in kwargs:
                handle = repo_handle.open_for_read_with_flags(branch, url_path.path, kwargs["flags"])
            else:
                handle = repo_handle.open_for_read(branch, url_path.path)
            return XetFile(handle)
        elif mode.startswith('w'):
            return self._transaction.open_for_write(url_path)
        else:
            raise ValueError("Mode '%s' not supported.", mode)

    def set_commit_message(self, message):
        """
        Sets the commit message on the active transaction
        """
        if self._transaction is None:
            raise RuntimeError("No active transaction")
        self._transaction.set_commit_message(message)

    def rm(self, path, *args, **kwargs):
        """
        Delete a file.

        Deletions must be performed within the context of a transaction 
        which must be scoped to within a single repository branch.
        """
        transaction = self._transaction

        if transaction is None:
            raise RuntimeError(
                "Write access to files is only allowed within a commit transaction.")

        if len(args) > 0:
            print(f"rm arguments {args} ignored", file=sys.stderr)
        if len(kwargs) > 0:
            print(f"rm arguments {kwargs} ignored", file=sys.stderr)

        path = parse_url(path, self.domain)
        if len(path.path) == 0 and len(path.branch) > 0:
            raise ValueError("Cannot delete branches with 'rm'")
        if len(path.path) == 0 and len(path.branch) == 0:
            raise ValueError("Cannot delete repositories with 'rm'")

        transaction.rm(path)

    def is_repo(self, path):
        """
        Returns true if the path is a repo
        """
        url_path = parse_url(path, self.domain)
        if len(url_path.branch) != 0:
            raise ValueError("Too many path components to be a repo")
        try:
            self.list_branches(path)
            return True
        except Exception as e:
            return False

    def make_branch(self, repo, src_branch_name, target_branch_name):
        """
        Creates a branch in a repo
        """
        if not self.is_repo(repo):
            raise ValueError(f"{repo} is not a repository")

        has_src_branch = self.branch_exists(repo + "/" + src_branch_name)
        if has_src_branch is False:
            raise ValueError(f"Cannot copy branch as source branch does not exist: {src_branch_name}")
        has_dest_branch = self.branch_exists(repo + "/" + target_branch_name)
        if has_dest_branch:
            raise ValueError(f"Cannot copy branch as destination branch already exists: {target_branch_name}")

        query = {"new_branch_name": target_branch_name,
                 "old_branch_name": src_branch_name}
        query = json.dumps(query)
        url_path = parse_url(repo, self.domain)
        _manager.api_query(url_path.remote, "branches", "post", query)

    def find_ref(self, repo, ref_name):
        if not self.is_repo(repo):
            raise ValueError(f"{repo} is not a repository")
        url_path = parse_url(repo, self.domain)
        res = _manager.api_query(url_path.remote, f"git/refs/{ref_name}", "get", "")
        return json.loads(bytes(res))

    def delete_branch(self, repo, branch_name):
        """
        deletes a branch in a repo
        """
        if not self.is_repo(repo):
            raise ValueError(f"{repo} is not a repository")

        has_branch = self.branch_exists(repo + "/" + branch_name)
        if has_branch is False:
            raise ValueError(f"Cannot delete branch as branch does not exist: {branch_name}")

        if branch_name == 'main':
            raise ValueError("Cannot delete main branch")

        url_path = parse_url(repo, self.domain)
        _manager.api_query(url_path.remote, f"branches/{branch_name}", "delete", "")

    def cp_file(self, path1, path2, *args, **kwargs):
        """
        Copies a file or directory from a xet path to another xet path.

        Copies must be performed within the context of a transaction
        and are allowed to span branches
        """
        transaction = self._transaction
        if len(args) > 0:
            print(f"cp arguments {args} ignored", file=sys.stderr)
        if len(kwargs) > 0:
            print(f"cp arguments {kwargs} ignored", file=sys.stderr)

        if transaction is None:
            raise RuntimeError(
                "Write access to files is only allowed within a commit transaction.")
        parsed_path1 = parse_url(path1, self.domain)
        parsed_path2 = parse_url(path2, self.domain)
        if parsed_path1.remote != parsed_path2.remote:
            raise ValueError("Can only copy between paths in the same repository")
        if len(parsed_path1.branch) == 0:
            raise ValueError(f"Branch not specified in copy source {path1}")
        if len(parsed_path2.branch) == 0:
            raise ValueError(f"Branch not specified in copy dest {path2}")

        if len(parsed_path1.path) == 0 and len(parsed_path2.path) == 0:
            query = {"new_branch_name": parsed_path2.branch,
                     "old_branch_name": parsed_path1.branch}
            query = json.dumps(query)
            _manager.api_query(parsed_path1.remote, "branches", "post", query)
            return

        transaction.copy(parsed_path1, parsed_path2)

    def mv(self, path1, path2, *args, **kwargs):
        """
        Moves a file or directory from a xet path to another xet path.

        Moves must be performed within the context of a transaction 
        and must be within the same branch
        """
        transaction = self._transaction
        if len(args) > 0:
            print(f"move arguments {args} ignored", file=sys.stderr)
        if len(kwargs) > 0:
            print(f"move arguments {kwargs} ignored", file=sys.stderr)

        if transaction is None:
            raise RuntimeError(
                "Write access to files is only allowed within a commit transaction.")
        parsed_path1 = parse_url(path1, self.domain)
        parsed_path2 = parse_url(path2, self.domain)
        if parsed_path1.branch != parsed_path2.branch:
            raise ValueError("Moves can only happen within a branch")

        transaction.mv(parsed_path1, parsed_path2)

    def move(self, path1, path2, *args, **kwargs):
        return self.mv(path1, path2, *args, **kwargs)

    def add_deduplication_hints(self, path_urls):
        """
        Fetches and downloads all of the metadata needed for binary deduplication against 
        all the paths given by `paths`.  Once fetched, new data will be deduplicated against 
        any binary content given by `paths`.  
        """
        if isinstance(path_urls, str):
            path_urls = [path_urls]
        url_paths = [parse_url(url, self.domain, partial_remote=True) for url in path_urls]

        self._add_deduplication_hints_by_url(url_paths)

    def _add_deduplication_hints_by_url(self, url_paths, min_dedup_byte_threshhold=None):

        if min_dedup_byte_threshhold is None:
            # TODO: set this to a more resonable value.  
            min_dedup_byte_threshhold = 0

        paths_by_remotes = {}
        for url in url_paths:
            paths_by_remotes.setdefault(url.remote, []).append(url)

        for (remote, urls) in paths_by_remotes.items():
            repo_handle = _manager.get_repo(remote)

            repo_handle.fetch_hinted_shards_for_dedup([(url.branch, url.path) for url in urls],
                                                      min_dedup_byte_threshhold)

    @property
    def transaction(self):
        """
        Begin a transaction context for a given repository and branch.
        The entire transaction is committed atomically at the end of the 
        transaction. All writes must be performed into this branch::

            with fs.transaction as tr:
                tr.set_commit_message("message")
                file = fs.open('user/repo/main/hello.txt','w')
                file.write('hello world')
                file.close()

        The transaction object is an instance of the :class:`MultiCommitTransaction`
        """
        if self._transaction is None:
            self._transaction = MultiCommitTransaction(self)
        return self._transaction

    def _create_transaction_handler(self, repo_info, commit_message):
        """
        Internal method used by CommitTransaction to get
        a transaction handler object from the repository handle
        """
        repo_handle = _manager.get_repo(repo_info.remote)
        return repo_handle.begin_write_transaction(repo_info.branch, commit_message)

    def start_transaction(self, commit_message=None):
        """
        Begin a write transaction for a repository and branch.
        The entire transaction is committed atomically at the end of the
        transaction. All writes must be performed into this branch

        repo_and_branch is of the form
        `user/repo/branch` or `xet://user/repo/branch`::

            fs.start_transaction('my commit message')
            file = fs.open('user/repo/main/hello.txt','w')
            file.write('hello world')
            file.close()
            fs.end_transaction()

        The transaction object is an instance of the :class:`MultiCommitTransaction`
        """
        tr = self.transaction
        tr.start()
        tr.set_commit_message(commit_message)
        return tr

    def cancel_transaction(self):
        """Cancels any active transactions. non-context version"""
        self._transaction.complete(False)

    def end_transaction(self):
        """Finish write transaction, non-context version. See :func:`start_transaction`"""
        self._transaction.complete()

    def mkdir(path, *args, **kwargs):
        """Noop. Empty directories cannot be created"""
        pass

    def mkdirs(path, *args, **kwargs):
        """Noop. Empty directories cannot be created"""
        pass

    def makedir(path, *args, **kwargs):
        """Noop. Empty directories cannot be created"""
        pass

    def makedirs(path, *args, **kwargs):
        """Noop. Empty directories cannot be created"""
        pass


fsspec.register_implementation("xet", XetFS, clobber=True)
