import os
import subprocess
import sys
import threading
import typing

import typer
from tabulate import tabulate
from typing_extensions import Annotated

from . import util
from .file_system import XetFS
from .sync import SyncCommand
from .url_parsing import parse_url
from .util import _get_fs_and_path, CHUNK_SIZE
from .file_operations import perform_copy
from .version import __version__

if 'SPHINX_BUILD' not in os.environ:
    from .rpyxet import rpyxet

cli = typer.Typer(add_completion=True, short_help="a pyxet command line interface", no_args_is_help=True)
repo = typer.Typer(add_completion=False, short_help="sub-commands to manage repositories")
branch = typer.Typer(add_completion=False, short_help="sub-commands to manage branches")

cli.add_typer(repo, name="repo")
cli.add_typer(branch, name="branch")


class PyxetCLI:
    @staticmethod
    @cli.command()
    def login(email: Annotated[str, typer.Option("--email", "-e", help="email address associated with account")],
              user: Annotated[str, typer.Option("--user", "-u", help="user name")],
              password: Annotated[str, typer.Option("--password", "-p", help="password")],
              host: Annotated[str, typer.Option("--host", "-h", help="host to authenticate against")] = "xethub.com",
              force: Annotated[bool, typer.Option("--force", "-f",
                                                  help="do not perform authentication check and force write to config")] = False,
              no_overwrite: Annotated[bool, typer.Option("--no_overwrite",
                                                         help="Do not overwrite if existing auth information is found")] = False):
        """
        Configures the login information. Stores the config in ~/.xetconfig
        """
        rpyxet.configure_login(host, user, email, password, force, no_overwrite)

    @staticmethod
    @cli.command()
    def mount(
            source: Annotated[str, typer.Argument(help="Repository and branch in format xet://[user]/[repo]/[branch]")],
            path: Annotated[str, typer.Argument(help="Path to mount to or a Windows drive letter)")],
            prefetch: Annotated[int, typer.Option(help="Prefetch blocks in multiple of 16MB. Default=32")] = 32):
        """
        Mounts a repository on a local path
        """
        fs = XetFS()
        source = parse_url(source, fs.domain)
        if source.path != '':
            raise ValueError("Cannot have a path when mounting. Expecting xet://[user]/[repo]/[branch]")
        if source.branch == '':
            raise ValueError("Branch or revision must be specified")
        if os.name == 'nt':
            # path must be a drive letter (X, or X: or X:\\)
            letter = path[0]
            if path != letter and path != letter + ':' and path != letter + ':\\':
                raise ValueError("Path must be a Windows drive letter in format X:")
            path = letter
        rpyxet.perform_mount(sys.executable, source.remote, path, source.branch, prefetch)

    @staticmethod
    @cli.command(name="mount-curdir", hidden=True)
    def mount_curdir(path: Annotated[str, typer.Argument(help="path to mount to")],
                     autostop: Annotated[
                         bool, typer.Option('--autostop', help="Automatically terminates on unmount")] = False,
                     reference: Annotated[
                         str, typer.Option('--reference', '-r', help="branch or revision to mount")] = 'HEAD',
                     prefetch: Annotated[int, typer.Option('--prefetch', '-p', help="prefetch aggressiveness")] = 32,
                     ip: Annotated[str, typer.Option('--ip', help="IP used to host the NFS server")] = "127.0.0.1",
                     writable: Annotated[bool, typer.Option('--writable', help="Experimental. Do not use")] = False,
                     signal: Annotated[int, typer.Option('--signal', help="Internal:Sends SIGUSR1 to this pid")] = -1):
        """
        Internal Do not use
        """
        rpyxet.perform_mount_curdir(path=path,
                                    reference=reference,
                                    signal=signal,
                                    autostop=autostop,
                                    prefetch=prefetch,
                                    ip=ip,
                                    writable=writable)

    @staticmethod
    @cli.command()
    def clone(source: Annotated[str, typer.Argument(help="Repository in format xet://[user]/[repo]")],
              args: Annotated[typing.List[str], typer.Argument(help="Arguments to be passed to git-xet clone")] = None):
        """
        Clones a repository on a local path
        """
        res = subprocess.run(["git-xet", "-V"], capture_output=True)
        if res.returncode != 0:
            print("git-xet not found. Please install git-xet from https://xethub.com/explore/install")
            return
        fs = XetFS()
        source = parse_url(source, fs.domain)
        commands = ["git-xet", "clone"] + [source.remote] + args
        strcommand = ' '.join(commands)
        print(f"Running '{strcommand}'")
        subprocess.run(["git-xet", "clone"] + [source.remote] + args)

    @staticmethod
    @cli.command()
    def version():
        """
        Prints the current Xet-cli version
        """
        print(__version__)

    @cli.command()
    def cp(source: Annotated[typing.List[str], typer.Argument(help="Source file or folder to copy")],
           target: Annotated[str, typer.Argument(help="Target location of the file or folder")],
           recursive: Annotated[
               bool, typer.Option("--recursive", "-r", help="Recursively copy files and folders ")] = False,
           message: Annotated[
               str, typer.Option("--message", "-m", help="A commit message")] = "",
           parallel: Annotated[
               int, typer.Option("--parallel", "-p", help="Maximum amount of parallelism")] = 32):
        """copy files and folders"""
        if len(source) == 0:
            raise ValueError("Empty source list")
        if not message:
            message = f"copy {', '.join(source[:3])}... to {target}" if not recursive else f"copy {', '.join(source[:3])}... to {target} recursively"
        util.MAX_CONCURRENT_COPIES = threading.Semaphore(parallel)
        perform_copy(source, target, message, recursive=recursive)

    @staticmethod
    @cli.command()
    def sync(source: Annotated[str, typer.Argument(help="Source folder to sync")],
             target: Annotated[str, typer.Argument(help="Target location of the folder")],
             use_mtime: Annotated[bool, typer.Option("--use-mtime", help="Use mtime as criteria for sync")] = False,
             message: Annotated[str, typer.Option("--message", "-m", help="A commit message")] = "",
             update_size: Annotated[bool, typer.Option("--update-size", hidden=True, help="Update Xetea with the size of the remote bucket")] = False,
             parallel: Annotated[int, typer.Option("--parallel", "-p", help="Maximum amount of parallelism")] = 32,
             dryrun: Annotated[
                 bool, typer.Option("--dryrun",
                                    help="Displays the operations that would be performed without actually running them")] = False):
        """Copy changed files from source to target"""
        if not message:
            message = f"sync {source} to {target}"
        util.MAX_CONCURRENT_COPIES = threading.Semaphore(parallel)
        cmd = SyncCommand(source, target, use_mtime, message, dryrun, update_size)
        print(f"Checking sync")
        cmd.validate()
        print(f"Starting sync")
        if dryrun:
            print("This is a dryrun")
        stats = cmd.run()
        if not dryrun:
            print(f"Completed sync. Copied: {stats.copied} files, ignored: {stats.ignored} files")
            if stats.failed > 0:
                print(f"{stats.failed} entries failed to copy")

    @staticmethod
    @cli.command()
    def ls(path: Annotated[str, typer.Argument(help="Source file or folder to list")] = "xet://",
           raw: Annotated[bool, typer.Option(help="If True, will print the raw JSON output")] = False):
        """list files and folders"""
        original_path = path
        fs, path = _get_fs_and_path(path)
        try:
            listing = fs.ls(path, detail=True)
            if raw:
                print(listing)
            else:
                if fs.protocol == 'xet':
                    for entry in listing:
                        entry['name'] = 'xet://' + entry['name']
                print(tabulate(listing, headers="keys"))
            return listing
        except Exception as e:
            # this failed to list. retry as a file
            if fs.protocol == 'xet':
                return PyxetCLI.info(original_path, raw)
            print(f"{e}")
            return

    @staticmethod
    @cli.command()
    def cat(path: Annotated[str, typer.Argument(help="Source file or folder to print")],
            limit: Annotated[int, typer.Option(help="Maximum number of bytes to print")] = 0):
        """Prints a file to stdout"""
        fs, path = _get_fs_and_path(path)
        try:
            file = fs.open(path, 'rb')
            if limit == 0:
                while True:
                    chunk = file.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    sys.stdout.buffer.write(chunk)
            else:
                chunk = file.read(limit)
                sys.stdout.buffer.write(chunk)
        except Exception as e:
            print(f"{e}")
            return

    @staticmethod
    @cli.command()
    def rm(paths: Annotated[typing.List[str], typer.Argument(help="File or folder to delete")],
           message: Annotated[
               str, typer.Option("--message", "-m", help="A commit message")] = ""):
        """delete files and folders"""
        if not message:
            message = f"delete {paths}"
        fs, _ = _get_fs_and_path(paths[0])
        try:
            destproto_is_xet = fs.protocol == 'xet'
            if destproto_is_xet:
                for path in paths:
                    parsed_path = parse_url(path, fs.domain)
                    if len(parsed_path.path) == 0 and len(parsed_path.branch) > 0:
                        print(
                            "Cannot delete branches with 'rm' as this is a non-reversible operation and history will not be preserved. Use 'xet branch del'",
                            file=sys.stderr)
                        return
                fs.start_transaction(message)
            for path in paths:
                fs.rm(path)
            if destproto_is_xet:
                fs.end_transaction()
        except Exception as e:
            print(f"{e}")
            return

    @staticmethod
    @cli.command()
    def mv(source: Annotated[str, typer.Argument(help="Source file or folder to move")],
           target: Annotated[str, typer.Argument(help="Target location or name to move to")],
           recursive: Annotated[
               bool, typer.Option("--recursive", "-r", help="Recursively copy files and folders ")] = False,
           message: Annotated[
               str, typer.Option("--message", "-m", help="A commit message")] = ""):
        """move files and folders"""
        if not message:
            message = f"move {source} to {target}" if not recursive else f"move {source} to {target} recursively"
        src_fs, src_path = _get_fs_and_path(source)
        dest_fs, dest_path = _get_fs_and_path(target)
        if src_fs.protocol != dest_fs.protocol:
            print(
                "Unable to move between different protocols {src_fs.protocol}, {dest_fs.protocol}\nYou may want to copy instead",
                file=sys.stderr)
        destproto_is_xet = dest_fs.protocol == 'xet'
        try:
            if destproto_is_xet:
                dest_fs.start_transaction(message)
            dest_fs.mv(src_path, dest_path)
            if destproto_is_xet:
                dest_fs.end_transaction()
        except Exception as e:
            print(f"{e}")
            return

    @staticmethod
    @cli.command()
    def info(uri: Annotated[str, typer.Argument(help="A URI in format xet://[user]/[repo]/[branch]")],
             raw: Annotated[bool, typer.Option(help="If True, will print the raw JSON output")] = False):
        """Provide information about any path"""
        fs, path = _get_fs_and_path(uri)
        try:
            info = fs.info(path)
            if raw:
                print(info)
            else:
                if fs.protocol == 'xet':
                    info['name'] = 'xet://' + info['name']
                print(tabulate([info], headers="keys"))
            return info
        except Exception as e:
            print(f"{e}")
            return

    @staticmethod
    @cli.command()
    def duplicate(source: Annotated[str, typer.Argument(help="Origin repo to fork from")],
                  dest: Annotated[str, typer.Argument(help="New repository name")] = None,
                  private: Annotated[bool, typer.Option('--private', help="make repository private")] = False,
                  public: Annotated[bool, typer.Option('--public', help="make repository public")] = False,
                  ):
        """
        Duplicates (via a detached fork) a copy of a repository from xet://[user]/[repo] to your own account.
        Defaults to original repository private/public settings. Use --private or --public to adjust the repository permissions. 
        If dest is not provided, xet://[yourusername]/[repo] is used.
        Use `xet fork` if you want a regular fork.
        """

        fs = XetFS()
        if dest is None:
            repo_name = source.rstrip('/').split('/')[-1]
            username = fs.get_username().strip()
            if not bool(username):
                print("Failed to infer a username to duplicate the repo, please provide a full target name")
                return
            dest = "xet://" + username + "/" + repo_name
            print(f"Duplicating to {dest}")
        else:
            repo_name = source.rstrip('/').split('/')[-1]
        fs.duplicate_repo(source, dest)
        try:
            if private:
                print(f"Duplicate successful. Changing permissions...")
                fs.set_repo_attr(dest, "private", True)
                print(f"Repo permissions set successfully")
            if public:
                print(f"Duplicate successful. Changing permissions...")
                fs.set_repo_attr(dest, "public", False)
                print(f"Repo permissions set successfully")
        except Exception as e:
            username = fs.get_username()
            print(f"An error has occurred setting repository permissions: {e}")
            print("Permission changes may not have been made. Please change it manually at:")
            print(f"  {fs.domain}/{username}/{repo_name}/settings")


class BranchCLI:
    @staticmethod
    @branch.command()
    def make(repo: Annotated[str, typer.Argument(help="Repository name in format xet://[user]/[repo]")],
             src_branch: Annotated[str, typer.Argument(help="Source branch to copy")],
             dest_branch: Annotated[str, typer.Argument(help="New branch name")]):
        """
        make a new branch copying another branch.
        Branch names with "/" are not supported.

        Example: Create a new branch from the main branch

            xet branch make xet://user/repo main new_branch
        """
        fs, remote = _get_fs_and_path(repo)
        assert (fs.protocol == 'xet')
        assert ('/' not in dest_branch)
        fs.make_branch(remote, src_branch, dest_branch)

    @staticmethod
    @branch.command()
    def ls(repo: Annotated[str, typer.Argument(help="Repository name in format xet://[user]/[repo]")],
           raw: Annotated[bool, typer.Option(help="If True, will print the raw JSON output")] = False):
        """
        list branches of a repository.
        """
        fs, path = _get_fs_and_path(repo)
        if fs.protocol != 'xet':
            print("Please specify a valid repository name in format xet://[user]/[repo]")
            return
        try:
            listing = fs.list_branches(repo, raw)
            listing = [{'name': "xet://" + path + '/' + n['name'], 'type': 'branch'} for n in listing]
            if raw:
                print(listing)
            else:
                print(tabulate(listing, headers="keys"))
            return listing
        except Exception as e:
            print(f"{e}")
            return

    @staticmethod
    @branch.command()
    def delete(repo: Annotated[str, typer.Argument(help="Repository name in format xet://[user]/[repo]")],
               branch: Annotated[str, typer.Argument(help="Branch to delete")],
               yes: Annotated[bool, typer.Option(help="Type yes to delete")] = False):
        """
        Deletes a branch. Note that this is not an easily reversible operation.
        """
        print("---------------------------------------------------", file=sys.stderr)
        print("                    WARNING", file=sys.stderr)
        print("---------------------------------------------------", file=sys.stderr)
        print("Branch deletion is not a easily reversible operation", file=sys.stderr)
        print("Any data which only exists on a branch will become irreversibly inaccessible", file=sys.stderr)
        print("", file=sys.stderr)
        if yes:
            print("--yes is set. Issuing deletion", file=sys.stderr)
            fs, path = _get_fs_and_path(repo)
            if fs.protocol != 'xet':
                print("Please specify a valid repository name xet://[user]/[repo]")
                return
            return fs.delete_branch(repo, branch)
        else:
            print("Add --yes to delete", file=sys.stderr)

    @staticmethod
    @branch.command()
    def info(repo: Annotated[str, typer.Argument(help="Repository name in format xet://[user]/[repo]")],
             branch: Annotated[str, typer.Argument(help="Branch to query")]):
        """
        Prints information about a branch
        """
        fs, path = _get_fs_and_path(repo)
        if fs.protocol != 'xet':
            print("Please specify a valid repository name in format xet://[user]/[repo]")
            return
        ret = fs.find_ref(repo, branch)
        print(ret)
        return ret


class RepoCLI:
    @staticmethod
    @repo.command()
    def make(name: Annotated[str, typer.Argument(help="Repository name in format xet://[user]/[repo]")],
             private: Annotated[bool, typer.Option('--private', help="Make repository private")] = False,
             public: Annotated[bool, typer.Option('--public', help="Make repository public")] = False,
             raw: Annotated[bool, typer.Option('--raw', help="Raw output")] = False,
             ):
        """
        make a new empty repository. Either --private or --public must be set
        """
        if private == public:
            print("Either --private or --public must be set")
            return
        fs = XetFS()
        ret = fs.make_repo(name, private=private)
        if raw:
            print(ret)
        else:
            htmlurl = ret['html_url']
            domain_split = htmlurl.split('://')[1]
            path_split = domain_split.split('/')[1:]
            path_split = '/'.join(path_split)
            xet_path = f'xet://{path_split}'
            if public:
                print(f"Public repository created at {xet_path}")
            elif private:
                print(f"Private repository created at {xet_path}")
        return ret

    @staticmethod
    @repo.command()
    def fork(source: Annotated[str, typer.Argument(help="Origin repo to fork from")],
             dest: Annotated[str, typer.Argument(help="New repository name")] = None,
             ):
        """
        Forks a copy of a repository from xet://[user]/[repo] to your own account.
        Defaults to original repository private/public settings. 
        If dest is not provided, xet://[yourusername]/[repo] is used.
        Use `xet duplicate` if you want a detached private fork.
        """

        fs = XetFS()
        if dest is None:
            repo_name = source.rstrip('/').split('/')[-1]
            dest = "xet://" + fs.get_username() + "/" + repo_name
            print(f"Forking to {dest}")
        fs.fork_repo(source, dest)

    @staticmethod
    @repo.command()
    def ls(raw: Annotated[bool, typer.Option(help="If True, will print the raw JSON output")] = False):
        """
        list repositories of a user.
        """
        fs = XetFS()
        try:
            repos = fs.list_repos(raw)
            if raw:
                print(repos)
            else:
                print(tabulate(repos, headers="keys"))
            return repos
        except Exception as e:
            print(f"{e}")
            return

    @staticmethod
    @repo.command()
    def rename(source: Annotated[str, typer.Argument(help="Origin repo to rename from in format xet://[user]/[repo])")],
               dest: Annotated[str, typer.Argument(help="Repo to rename to in format xet://[user]/[repo]")]):
        """
        Forks a new repository from an existing repository.
        """
        fs = XetFS()
        fs.rename_repo(source, dest)

    @staticmethod
    @repo.command()
    def clone(source: Annotated[str, typer.Argument(help="Repository in format xet://[user]/[repo]")],
              args: Annotated[typing.List[str], typer.Argument(help="Arguments to be passed to git-xet clone")] = None):
        """
        Clones a repository on a local path
        """
        return PyxetCLI.clone(source, args)

    @staticmethod
    @repo.command()
    def info(uri: Annotated[str, typer.Argument(help="A URI in format xet://[user]/[repo]")]):
        """
        provide information on the repo\n

        response:\n
        - name: str
        - description: str
        - private: bool
        - created_at: datetime
        - updated_at: datetime
        - pushed_at: datetime
        - branch: str
        - commit_id: str
        - commit_message: str
        - materialize: float
        - stored: float


        """
        raise NotImplementedError("info command is not implemented yet")
