import sys
import threading

import fsspec

from .file_interface import XetFile

TRANSACTION_FILE_LIMIT = 512


def _validate_repo_info_for_transaction(repo_info):
    if repo_info.remote == '':
        raise ValueError("No repository specified")
    if repo_info.branch == '':
        raise ValueError("No branch specified")

def repo_info_key(repo_info):
    return f"{repo_info.remote}/{repo_info.branch}"


class MultiCommitTransaction(fsspec.transaction.Transaction):
    """
    Handles a commit using the transaction interface.
    This transaction handler supports transactions across multiple branches
    by tracking them separately. Simultaneous changes across branches
    will require multiple actual transactions to complete.
    """

    def __init__(self, fs, commit_message=None):
        """
        This class should not be used directly.
        It is preferred to use fs.transaction.
        """
        self.commit_message = None
        self._transaction_pool = {}
        self.fs = fs
        self._set_commit_message(commit_message)
        self.lock = threading.Lock()

        super().__init__(fs)

    def set_commit_message(self, commit_message):
        """
        Sets the commit message to be used. This applies to every
        current uncommitted transaction and future transactions.
        If commit_message is None, a default message "Commit [current datetime]"
        is used.
        """
        with self.lock:
            self._set_commit_message(commit_message)

    def _set_commit_message(self, commit_message):
        if commit_message is None:
            import datetime
            commit_message = "Commit " + datetime.datetime.now().isoformat()
        self.commit_message = commit_message

    def __repr__(self):
        with self.lock:
            return f"MultiCommitTransaction for [{self._transaction_pool.keys()}]"

    def __str__(self):
        with self.lock:
            return f"MultiCommitTransaction for [{self._transaction_pool.keys()}]"

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End transaction and commit, if exit is not due to exception"""
        # only commit if there was no exception
        self.complete(commit=exc_type is None)

    def get_handler_for_repo_info(self, repo_info):
        with self.lock:
            key = repo_info_key(repo_info)

            try:
                tr = self._transaction_pool[key]

                if tr.transaction_size() >= TRANSACTION_FILE_LIMIT:
                    tr.commit_and_restart()

            except KeyError:
                tr = self.fs._create_transaction_handler(repo_info, self.commit_message) 
                self._transaction_pool[key] = tr

            return tr.create_access_token()

    def open_for_write(self, repo_info):
        """
        Opens a file for writing.
        `repo_info` is the result of `pyxet.parse_url(url)`
        """
        handler = self.get_handler_for_repo_info(repo_info)
        return XetFile(handler.open_for_write(repo_info.path), handler)

    def start(self):
        """
        Starts the transaction
        """
        if self.fs.intrans:
            raise RuntimeError("Transaction already in progress")
        self.fs.intrans = True

    def complete(self, commit=True):
        """
        Finalizes and commits or cancels this transaction.
        The transaction can be restarted with start()
        """
        with self.lock:  # Should not be called while other things are in progress, but better be safe.
            ret_except = None
            for k, v in self._transaction_pool.items():
                try:
                    v.complete(commit)
                except Exception as e:
                    sys.stderr.write(f"Failed to commit {k}: {e}\n")
                    sys.stderr.flush()
                    if ret_except is None:
                        ret_except = e
            # reset all the transaction state
            self._transaction_pool = {}
            self.fs.intrans = False
            self._set_commit_message(None)
            if ret_except is not None:
                raise ret_except

    def copy(self, src_repo_info, dest_repo_info):
        """
        Copies a file from src to dest.
        src_repo_info and dest_repo_info are the returned values from
        `pyxet.parse_url(url)`
        """
        handler = self.get_handler_for_repo_info(dest_repo_info)
        handler.copy(src_repo_info.branch, src_repo_info.path, dest_repo_info.path)

    def mv(self, src_repo_info, dest_repo_info):
        """
        Moves a file from src to dest.
        src_repo_info and dest_repo_info are the returned values from
        `pyxet.parse_url(url)`
        """
        handler = self.get_handler_for_repo_info(dest_repo_info)
        handler.mv(src_repo_info.path, dest_repo_info.path)

    def rm(self, repo_info):
        """
        Removes a file.
        repo_info is the return value of `pyxet.parse_url(url)`
        """
        handler = self.get_handler_for_repo_info(repo_info)
        handler.delete(repo_info.path)

    def _set_do_not_commit(self):
        """
        Internal method for testing purposes.
        Flags all active transactions to not attempt to push
        the commit, but will silently succeed.
        """
        with self.lock:
            for v in self._transaction_pool.values():
                v.set_do_not_commit()

    def _set_error_on_commit(self):
        """
        Internal method for testing purposes.
        Flags all active transactions to not attempt to push
        the commit, but will just raise an exception
        """
        with self.lock:
            for v in self._transaction_pool.values():
                v.set_error_on_commit()

    def get_change_list(self):
        deletes = []
        new_files = []
        copies = []
        moves = []
        for v in self._transaction_pool.values():
            deletes.extend(v.deletes)
            new_files.extend(v.new_files)
            copies.extend(v.copies)
            moves.extend(v.moves)

        return {'deletes': deletes, 'new_files': new_files, 'copies': copies, 'moves': moves}
