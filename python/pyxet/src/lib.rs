use anyhow::{anyhow, Result};
use gitxetcore::command::login::*;
use gitxetcore::command::mount::*;
use gitxetcore::command::*;
use gitxetcore::config::ConfigGitPathOption;
use gitxetcore::config::ConfigGitPathOption::NoPath;
use gitxetcore::config::XetConfig;
use gitxetcore::environment::log::initialize_tracing_subscriber;
use progress_reporting::DataProgressReporter;
use pyo3::exceptions::*;
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyList};
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::{error, info};
use xetblob::*;

mod transactions;
use transactions::*;

#[pyclass]
pub struct FileAttributes {
    #[pyo3(get)]
    pub ftype: String, // "directory"/"file"/"symlink"
    #[pyo3(get)]
    pub size: usize,
    #[pyo3(get)]
    pub last_modified: String,
}

impl From<DirEntry> for FileAttributes {
    fn from(ent: DirEntry) -> Self {
        let ftype = match ent.object_type.as_str() {
            "dir" => "directory",
            "blob" => "file",
            "blob+exec" => "file",
            "link" => "symlink",
            "branch" => "branch",
            "repo" => "repo",
            _ => "file",
        };
        FileAttributes {
            ftype: ftype.to_string(),
            size: ent.size as usize,
            last_modified: ent.last_modified,
        }
    }
}

fn anyhow_to_runtime_error(e: anyhow::Error) -> PyErr {
    PyRuntimeError::new_err(format!("{e:?}"))
}

// This macro will release the GIL when called.
macro_rules! rust_async {
    ($py:ident, $xp:expr) => {{
        let mut res = Option::<Result<_, _>>::None;
        let mut err_res = Option::<anyhow::Error>::None;
        $py.allow_threads(|| {
            if let Err(e) = pyo3_asyncio::tokio::get_runtime().block_on(async {
                let r: Result<_, _> = { $xp };
                res = Some(r.map_err(|e| PyRuntimeError::new_err(format!("{e:?}"))));

                anyhow::Ok(())
            }) {
                err_res = Some(e);
            }
        });

        let ret: PyResult<_> = {
            if let Some(e) = err_res {
                Err(anyhow_to_runtime_error(e))
            } else {
                res.unwrap_or(Err(PyRuntimeError::new_err(
                    "Failed to block on tokio runtime",
                )))
            }
        };

        ret
    }};
}

#[pyclass]
struct PyRepoManager {
    manager: RwLock<XetRepoManager>,
}

#[pyfunction]
pub fn configure_login(
    host: String,
    user: String,
    email: String,
    token: String,
    no_auth: bool,
    no_overwrite: bool,
    py: Python<'_>,
) -> PyResult<()> {
    rust_async!(py, {
        let command = Command::Login(LoginArgs {
            host,
            user,
            email,
            password: token,
            force: no_auth,
            no_overwrite,
        });
        let config = XetConfig::new(None, None, NoPath)
            .map_err(|x| anyhow!("Unable to obtain default config: {x:?}"))?;

        command.run(config).await
    })
}

#[pyfunction]
pub fn perform_mount(
    python_exe: String,
    remote: String,
    path: String,
    reference: String,
    prefetch: Option<usize>,
    py: Python<'_>,
) -> PyResult<()> {
    let prefetch = prefetch.unwrap_or(2);
    #[cfg(target_os = "windows")]
    let command = {
        if path.len() > 1 {
            return Err(PyValueError::new_err("Target path must be a drive letter"));
        }
        Command::Mount(MountArgs {
            remote,
            drive: path,
            reference,
            foreground: false,
            prefetch,
            ip: "auto".to_string(),
            clonepath: None,
            writable: false,
            invoked_from_python: Some(python_exe),
            watch: None,
        })
    };

    #[cfg(not(target_os = "windows"))]
    let command = Command::Mount(MountArgs {
        remote,
        path: Some(std::path::PathBuf::from(&path)),
        reference,
        foreground: false,
        prefetch,
        ip: "127.0.0.1".to_string(),
        clonepath: None,
        writable: false,
        invoked_from_python: Some(python_exe),
        watch: None,
    });
    let config = XetConfig::new(None, None, NoPath)
        .map_err(|x| anyhow!("Unable to obtain default config {x}"))
        .map_err(anyhow_to_runtime_error)?;

    rust_async!(py, command.run(config).await)
}

#[pyfunction]
pub fn perform_mount_curdir(
    path: std::path::PathBuf,
    reference: String,
    signal: i32,
    autostop: bool,
    prefetch: usize,
    ip: String,
    writable: bool,
    py: Python<'_>,
) -> PyResult<()> {
    let signal = if signal < 0 { None } else { Some(signal) };
    let command = Command::MountCurdir(MountCurdirArgs {
        path,
        reference,
        signal,
        autostop,
        prefetch,
        ip,
        writable,
        watch: None,
    });

    let config = XetConfig::new(None, None, ConfigGitPathOption::CurdirDiscover)
        .map_err(|x| anyhow!("Unable to obtain default config {x}"))
        .map_err(anyhow_to_runtime_error)?;

    rust_async!(py, command.run(config).await)
}

#[pymethods]
impl PyRepoManager {
    #[new]
    pub fn new() -> PyResult<Self> {
        let manager = XetRepoManager::new(None, None).map_err(anyhow_to_runtime_error)?;
        Ok(PyRepoManager {
            manager: RwLock::new(manager),
        })
    }

    // return the current user name
    pub fn get_inferred_username(&self, remote: &str, py: Python<'_>) -> PyResult<String> {
        rust_async!(
            py,
            self.manager
                .read()
                .await
                .get_inferred_username(remote)
                .map_err(anyhow_to_runtime_error)
        )
    }

    /// Performs a file listing.
    pub fn listdir(
        &self,
        remote: &str,
        branch: &str,
        path: &str,
        py: Python<'_>,
    ) -> PyResult<(Vec<String>, Vec<FileAttributes>)> {
        // strip trailing slashes
        rust_async!(
            py,
            {
                #![allow(clippy::manual_strip)]
                let path = if path.ends_with('/') {
                    &path[..path.len() - 1]
                } else {
                    path
                };
                let path = if path.starts_with('/') {
                    &path[1..]
                } else {
                    path
                };
                let listing = self
                    .manager
                    .read()
                    .await
                    .listdir(remote, branch, path)
                    .await?;
                let mut ret_names = vec![];
                let mut ret_attrs = vec![];
                for i in listing {
                    if path.is_empty() {
                        ret_names.push(i.name.clone());
                    } else {
                        ret_names.push(format!("{path}/{}", i.name).to_string());
                    }
                    ret_attrs.push(i.into());
                }

                anyhow::Ok((ret_names, ret_attrs))
            }
        )
    }

    /// Performs a general api query.
    pub fn api_query(
        &self,
        remote: &str,
        op: &str,
        http_command: &str,
        body: &str,
        py: Python<'_>,
    ) -> PyResult<Vec<u8>> {
        rust_async!(
            py,
            self.manager
                .read()
                .await
                .perform_api_query(remote, op, http_command, body)
                .await
        )
    }

    /// Gets status of a path
    pub fn override_login_config(
        &self,
        user_name: &str,
        user_token: &str,
        email: Option<&str>,
        host: Option<&str>,
        py: Python<'_>,
    ) -> PyResult<()> {
        rust_async!(
            py,
            self.manager
                .write()
                .await
                .override_login_config(user_name, user_token, email, host)
                .await
        )
    }

    /// Gets status of a path
    pub fn stat(
        &self,
        remote: &str,
        branch: &str,
        path: &str,
        py: Python<'_>,
    ) -> PyResult<Option<FileAttributes>> {
        let ent: Option<DirEntry> = rust_async!(
            py,
            self.manager.read().await.stat(remote, branch, path).await
        )?;

        Ok(ent.map(|x| x.into()))
    }

    /// Obtains access to a repo
    pub fn get_repo(&self, remote: &str, py: Python<'_>) -> PyResult<PyRepo> {
        rust_async!(py, {
            let repo = self.manager.write().await.get_repo(None, remote).await?;
            anyhow::Ok(PyRepo { repo })
        })
    }
}
#[pyclass]
struct PyRepo {
    repo: Arc<XetRepo>,
}

#[pymethods]
impl PyRepo {
    pub fn open_for_read(&self, branch: &str, path: &str, py: Python<'_>) -> PyResult<PyRFile> {
        rust_async!(
            py,
            PyRFile::new(self.repo.open_for_read(branch, path, None).await?)
        )
    }

    pub fn open_for_read_with_flags(
        &self,
        branch: &str,
        path: &str,
        flags: u32,
        py: Python<'_>,
    ) -> PyResult<PyRFile> {
        rust_async!(
            py,
            PyRFile::new(self.repo.open_for_read(branch, path, Some(flags)).await?)
        )
    }

    pub fn begin_write_transaction(
        &self,
        branch: &str,
        commit_message: &str,
        py: Python<'_>,
    ) -> PyResult<PyWriteTransaction> {
        rust_async!(
            py,
            PyWriteTransaction::new(self.repo.clone(), branch, commit_message,).await
        )
    }

    /// Fetch shards that could be useful for dedup, according to the given endpoints.
    ///
    /// Endpoints are given as a list of (branch, path) tuples.  Shard hint fetches may be
    /// across branches.
    ///
    /// If min_num_bytes_in_dedup is specified, then only shards that collectively define
    /// that many bytes are actually downloaded; any hinted shards that don't specify at least
    /// the minimum number of bytes in chunks are ignored.
    ///
    pub fn fetch_hinted_shards_for_dedup(
        &self,
        file_paths: Vec<(&str, &str)>,
        min_dedup_bytes_for_shard_downloading: Option<usize>,
        py: Python<'_>,
    ) -> PyResult<()> {
        rust_async!(
            py,
            self.repo
                .fetch_hinted_shards_for_dedup(
                    &file_paths,
                    min_dedup_bytes_for_shard_downloading.unwrap_or(0)
                )
                .await
        )
    }
}

#[pyclass(subclass)]
#[derive(Clone)]
struct PyRFile {
    reader: XetRFileObject,
    pos: u64,
    file_len: u64,
    #[pyo3(get)]
    pub closed: bool,
}

const MAX_READ_SIZE: u64 = 8 * 1024 * 1024; // 8MB
impl PyRFile {
    fn new(reader: XetRFileObject) -> PyResult<PyRFile> {
        let len = reader.len();
        Ok(PyRFile {
            reader,
            pos: 0,
            file_len: len as u64,
            closed: false,
        })
    }

    pub async fn readline_impl(&mut self, size: i64) -> anyhow::Result<Vec<u8>> {
        let mut ret = Vec::new();
        let mut done: bool = false;
        while !done {
            let read_size = if size < 0 {
                // if size is unlimited, we read in 4K blocks
                MAX_READ_SIZE as i64
            } else {
                // otherwise 4K or size - amount_read_so_far
                std::cmp::min(size - (ret.len() as i64), MAX_READ_SIZE as i64)
            };
            // if we run out of read we are done
            if read_size <= 0 {
                break;
            }

            // actually do the read
            let fs_read = self.reader.read(self.pos, read_size as u32).await?;
            let (mut buf, eof) = fs_read;
            // if we find the '\n' we append that to the return buffer and flag done
            if let Some(pos) = buf.iter().position(|&x| x == b'\n') {
                ret.extend_from_slice(&buf[..pos + 1]);
                // shift the read position to after the \n
                self.pos += (pos + 1) as u64;
                done = true;
            } else {
                // if we do not find the '\n' we just append to the return buffer
                // shift the read position to after the buffer
                // done only if we EOF
                self.pos += buf.len() as u64;
                ret.append(&mut buf);
                done = eof;
            }
        }

        Ok(ret)
    }

    pub async fn read_impl(&mut self, size: u32) -> anyhow::Result<Vec<u8>> {
        let fs_read = self.reader.read(self.pos, size).await?;

        let (buf, _) = fs_read;
        self.pos += buf.len() as u64;
        Ok(buf)
    }
}

#[pymethods]
impl PyRFile {
    pub fn is_closed(&self) -> PyResult<bool> {
        Ok(self.closed)
    }
    pub fn close(&mut self) -> PyResult<()> {
        self.closed = true;
        Ok(())
    }
    pub fn readable(&self) -> PyResult<bool> {
        Ok(!self.closed)
    }
    pub fn seekable(&self) -> PyResult<bool> {
        Ok(true)
    }
    pub fn writable(&self) -> PyResult<bool> {
        Ok(false)
    }
    pub fn tell(&self) -> PyResult<u64> {
        Ok(self.pos)
    }
    #[pyo3(signature = (offset, whence=0))]
    pub fn seek(&mut self, offset: i64, whence: usize) -> PyResult<u64> {
        const SEEK_SET: usize = 0;
        const SEEK_CUR: usize = 1;
        const SEEK_END: usize = 2;
        match whence {
            SEEK_SET => {
                self.pos = offset as u64;
            }
            SEEK_CUR => {
                self.pos = (self.pos as i64 + offset) as u64;
            }
            SEEK_END => {
                self.pos = ((self.file_len as i64) + offset) as u64;
            }
            _ => {
                return Err(PyValueError::new_err("Invalid Seek Whence"));
            }
        }
        if self.pos >= self.file_len {
            self.pos = self.file_len;
        }
        Ok(self.pos)
    }
    // why does IOBase have readline? this is not very nice.
    #[pyo3(signature = (size=-1))]
    pub fn readline(&mut self, size: i64, py: Python<'_>) -> PyResult<PyObject> {
        let ret = rust_async!(py, self.readline_impl(size).await)?;
        Ok(PyBytes::new(py, &ret).into())
    }

    #[pyo3(signature = (num_lines=-1))]
    pub fn readlines(&mut self, num_lines: i64, py: Python<'_>) -> PyResult<PyObject> {
        let v_buf = rust_async!(py, {
            let mut v_buf = Vec::new();

            while num_lines <= 0 || (v_buf.len() as i64) < num_lines {
                let buf = self.readline_impl(-1).await?;
                if !buf.is_empty() {
                    v_buf.push(buf);
                } else {
                    break;
                }
            }
            anyhow::Ok(v_buf)
        })?;

        let ret = PyList::empty(py);

        for buf in v_buf {
            ret.append(PyBytes::new(py, &buf))?;
        }
        Ok(ret.into())
    }

    #[pyo3(signature = (size=-1))]
    pub fn read(&mut self, size: i64, py: Python<'_>) -> PyResult<PyObject> {
        // if size is <=0, its basically readall
        if size <= 0 {
            return self.readall(py);
        }
        let ret = rust_async!(py, {
            let size = std::cmp::min(size as u64, u32::MAX as u64);
            self.read_impl(size as u32).await
        })?;
        Ok(PyBytes::new(py, &ret).into())
    }

    pub fn readall(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        let ret = rust_async!(py, {
            let mut ret = Vec::new();
            while self.pos < self.file_len {
                ret.extend(self.read_impl(MAX_READ_SIZE as u32).await?);
            }
            anyhow::Ok(ret)
        })?;
        Ok(PyBytes::new(py, &ret).into())
    }
    pub fn readinto1(&mut self, b: &PyAny, py: Python<'_>) -> PyResult<u64> {
        let buf = PyByteArray::from(py, b)?;
        let buflen = buf.len();
        let bufbytes = unsafe { buf.as_bytes_mut() };

        rust_async!(py, {
            let read_size = std::cmp::min(buflen as u64, MAX_READ_SIZE);
            let readres = self.read_impl(read_size as u32).await?;
            let readlen = readres.len();

            if readlen > 0 {
                bufbytes[..readlen].copy_from_slice(&readres);
            }
            anyhow::Ok(readlen as u64)
        })
    }

    pub fn readinto(&mut self, b: &PyAny, py: Python<'_>) -> PyResult<u64> {
        let buf = PyByteArray::from(py, b)?;
        let buflen = buf.len();
        let bufbytes = unsafe { buf.as_bytes_mut() };

        rust_async!(py, {
            let mut curoff: usize = 0;
            while self.pos < self.file_len {
                let read_size = std::cmp::min(buflen - curoff, MAX_READ_SIZE as usize);
                let readres = self.read_impl(read_size as u32).await?;
                let readlen = readres.len();

                bufbytes[curoff..curoff + readlen].copy_from_slice(&readres);
                curoff += readlen;
            }
            anyhow::Ok(curoff as u64)
        })
    }
    pub fn get(&mut self, path: &str, py: Python<'_>) -> PyResult<()> {
        rust_async!(py, {
            self.reader.get(path).await?;
            anyhow::Ok(())
        })
    }
    pub fn write(&mut self, _b: &PyAny, _py: Python<'_>) -> PyResult<()> {
        Err(PyRuntimeError::new_err("Readonly file"))
    }

    pub fn __copy__(&self) -> PyResult<PyRFile> {
        Ok(self.clone())
    }
    pub fn __deepcopy__(&self) -> PyResult<PyRFile> {
        Ok(self.clone())
    }
}

// This functions as a reference to access the internal transaction object.
// The PyWriteTransaction holds one handle, and each file open for writing
// holds one handle. Once all references are finished, then the transaction
// is either committed or canceled.
//
// The lock inside is an RwLock object so that other objects can quickly check
// whether a transaction has been canceled, allowing errors to propegate correctly.
//
// There are two paths for handles being released: explicitly, with proper error handling
// and reporting, and on drop, where errors are logged and ignored.  This intermediate
// class is needed to properly implement both semantics, so that errors on explicit
// completion propegate properly and transactions are not left in a bad state if there are
// errors elsewhere.
//
#[pyclass(subclass)]
#[derive(Clone)]
pub struct PyWriteTransaction {
    pwt: Option<Arc<RwLock<WriteTransaction>>>,

    repo: Arc<XetRepo>,
    branch: String,
    commit_message: String,
}

impl PyWriteTransaction {
    async fn new(repo: Arc<XetRepo>, branch: &str, commit_message: &str) -> Result<Self> {
        Ok(Self {
            pwt: Some(WriteTransaction::new(&repo, branch, commit_message).await?),
            repo,
            branch: branch.to_string(),
            commit_message: commit_message.to_string(),
        })
    }

    fn access_inner<'a>(&'a self) -> Result<&'a Arc<RwLock<WriteTransaction>>> {
        if let Some(t) = self.pwt.as_ref() {
            Ok(t)
        } else {
            // This means we've called close() on the transaction, then tried to use it.
            Err(anyhow!(
                "Transaction operation attempted after transaction completed."
            ))
        }
    }

    async fn complete_impl(&mut self, commit: bool, cleanup_immediately: bool) -> Result<()> {
        let Some(tr) = self.pwt.take() else {
            // This means either we've called close() on the transaction, then tried to use it;
            // or all associated write files complete and close before calling close().
            // Either case this should be a NOP.
            info!("Complete called after PyTransaction object committed");
            return Ok(());
        };

        {
            let mut trw = tr.write().await;

            if commit {
                trw.set_commit_when_ready();
            } else {
                trw.set_cancel_flag();
            }

            if cleanup_immediately {
                // If there is only one count, this will shut down the transaction
                // and propegate any errors
                trw.complete().await?;
            }
        }

        WriteTransaction::release_write_token(tr).await?;

        Ok(())
    }

    async fn commit_and_restart_impl(&mut self) -> Result<()> {
        // Complete the current transaction, but not blocking.
        self.complete_impl(true, false).await?;

        // Create a new write transaction wrapper
        self.pwt =
            Some(WriteTransaction::new(&self.repo, &self.branch, &self.commit_message).await?);

        Ok(())
    }
}

impl Drop for PyWriteTransaction {
    fn drop(&mut self) {
        // This should only occurs in case of errors elsewhere, but must be cleaned up okay.
        if let Some(handle) = self.pwt.take() {
            pyo3_asyncio::tokio::get_runtime().block_on(async {
                let res = WriteTransaction::release_write_token(handle).await;
                if let Err(e) = res {
                    error!("Error deregistering write handle in transaction : {e:?}");
                }
            });
        }
    }
}

//  The python interface for PyWriteTransaction.
#[pymethods]
impl PyWriteTransaction {
    pub fn complete(&mut self, commit: bool, py: Python<'_>) -> PyResult<()> {
        rust_async!(py, self.complete_impl(commit, true).await)
    }

    pub fn commit_and_restart(&mut self, py: Python<'_>) -> PyResult<()> {
        rust_async!(py, self.commit_and_restart_impl().await)
    }

    pub fn create_access_token(&self) -> PyResult<PyWriteTransactionAccessToken> {
        Ok(PyWriteTransactionAccessToken {
            tr: Some(
                self.access_inner()
                    .map_err(anyhow_to_runtime_error)?
                    .clone(),
            ),
        })
    }

    pub fn transaction_size(&self, py: Python<'_>) -> PyResult<usize> {
        rust_async!(
            py,
            self.access_inner()?.read().await.transaction_size().await
        )
    }

    pub fn set_cancel_flag(&self, py: Python<'_>) -> PyResult<()> {
        rust_async!(
            py,
            anyhow::Ok(self.access_inner()?.write().await.set_cancel_flag())
        )
    }

    /// This is for testing
    pub fn set_do_not_commit(&self, py: Python<'_>) -> PyResult<()> {
        rust_async!(
            py,
            anyhow::Ok(self.access_inner()?.write().await.set_do_not_commit())
        )
    }

    /// This is for testing
    pub fn set_error_on_commit(&self, py: Python<'_>) -> PyResult<()> {
        rust_async!(
            py,
            anyhow::Ok(self.access_inner()?.write().await.set_error_on_commit())
        )
    }

    #[getter]
    pub fn new_files(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        rust_async!(
            py,
            anyhow::Ok(self.access_inner()?.read().await.new_files.clone())
        )
    }

    #[getter]
    pub fn copies(&self, py: Python<'_>) -> PyResult<Vec<(String, String)>> {
        rust_async!(
            py,
            anyhow::Ok(self.access_inner()?.read().await.copies.clone())
        )
    }

    #[getter]
    pub fn deletes(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        rust_async!(
            py,
            anyhow::Ok(self.access_inner()?.read().await.deletes.clone())
        )
    }

    #[getter]
    pub fn moves(&self, py: Python<'_>) -> PyResult<Vec<(String, String)>> {
        rust_async!(
            py,
            anyhow::Ok(self.access_inner()?.read().await.moves.clone())
        )
    }
}

#[pyclass(subclass)]
#[derive(Clone)]
pub struct PyWriteTransactionAccessToken {
    tr: Option<Arc<RwLock<WriteTransaction>>>,
}

impl PyWriteTransactionAccessToken {
    async fn access_transaction_for_write<'a>(
        &'a self,
    ) -> Result<RwLockWriteGuard<'a, WriteTransaction>> {
        let Some(t) = &self.tr else {
            // This should only happen if it's been closed explicitly, then
            // access is attempted.
            return Err(anyhow!(
                "Transaction accessed for write after being closed."
            ));
        };

        Ok(t.write().await)
    }

    async fn access_transaction_for_read<'a>(
        &'a self,
    ) -> Result<RwLockReadGuard<'a, WriteTransaction>> {
        let Some(t) = &self.tr else {
            // This should only happen if it's been closed explicitly, then
            // access is attempted.
            return Err(anyhow!("Transaction accessed for read after being closed."));
        };

        Ok(t.read().await)
    }

    // release the handle.
    async fn release(&mut self) -> Result<()> {
        if let Some(handle) = self.tr.take() {
            WriteTransaction::release_write_token(handle).await?;
        }
        Ok(())
    }
}

impl Drop for PyWriteTransactionAccessToken {
    fn drop(&mut self) {
        // This should only occurs in case of errors elsewhere, but must be cleaned up okay.
        if let Some(handle) = self.tr.take() {
            pyo3_asyncio::tokio::get_runtime().block_on(async {
                let res = WriteTransaction::release_write_token(handle).await;
                if let Err(e) = res {
                    error!("Error deregistering transaction write token: {e:?}");
                }
            });
        }
    }
}

#[pymethods]
impl PyWriteTransactionAccessToken {
    pub fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        // This just allows errors that may happen when a transaction is committed.
        rust_async!(py, {
            if let Some(handle) = self.tr.take() {
                WriteTransaction::release_write_token(handle).await?;
            }
            anyhow::Ok(())
        })
    }

    pub fn open_for_write(&self, path: &str, py: Python<'_>) -> PyResult<PyWFile> {
        rust_async!(py, {
            let writer = self
                .access_transaction_for_write()
                .await?
                .open_for_write(path)
                .await?;

            anyhow::Ok(PyWFile {
                writer,
                transaction_write_handle: self.clone(),
            })
        })
    }

    pub fn delete(&self, path: &str, py: Python<'_>) -> PyResult<()> {
        rust_async!(
            py,
            self.access_transaction_for_write()
                .await?
                .delete(path)
                .await
        )
    }

    pub fn copy(
        &self,
        src_branch: &str,
        src_path: &str,
        target_path: &str,
        py: Python<'_>,
    ) -> PyResult<()> {
        rust_async!(
            py,
            self.access_transaction_for_write()
                .await?
                .copy(src_branch, src_path, target_path)
                .await
        )
    }

    pub fn mv(&self, src_path: &str, target_path: &str, py: Python<'_>) -> PyResult<()> {
        rust_async!(
            py,
            self.access_transaction_for_write()
                .await?
                .mv(src_path, target_path)
                .await
        )
    }

    pub fn __copy__(&self) -> PyResult<Self> {
        Ok(self.clone())
    }

    pub fn __deepcopy__(&self) -> PyResult<Self> {
        Ok(self.clone())
    }
}

#[pyclass(subclass)]
#[derive(Clone)]
pub struct PyWFile {
    writer: Arc<XetWFileObject>,
    transaction_write_handle: PyWriteTransactionAccessToken,
}

#[pymethods]
impl PyWFile {
    pub fn is_closed(&self, py: Python<'_>) -> PyResult<bool> {
        rust_async!(py, anyhow::Ok(self.writer.is_closed().await))
    }
    pub fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        rust_async!(py, {
            self.writer.close().await?;

            // Give back the handle explicitly in order to ensure errors
            // can get propegated properly here.
            self.transaction_write_handle.release().await
        })
    }

    pub fn write(&mut self, b: &PyAny, py: Python<'_>) -> PyResult<()> {
        let buf = PyByteArray::from(py, b)?;
        let bufbytes = unsafe { buf.as_bytes() };
        rust_async!(py, {
            if self
                .transaction_write_handle
                .access_transaction_for_read()
                .await?
                .commit_canceled
            {
                return Err(anyhow!("Write terminated as transaction was canceled."));
            }

            self.writer.write(bufbytes).await
        })
    }
    pub fn readable(&self) -> PyResult<bool> {
        Ok(false)
    }
    pub fn seekable(&self) -> PyResult<bool> {
        Ok(false)
    }
    pub fn writable(&self) -> PyResult<bool> {
        Ok(true)
    }

    pub fn __copy__(&self) -> PyResult<Self> {
        Ok(self.clone())
    }

    pub fn __deepcopy__(&self) -> PyResult<Self> {
        Ok(self.clone())
    }
}

#[pyclass(subclass)]
#[derive(Clone)]
pub struct PyProgressReporter {
    dpr: Arc<DataProgressReporter>,
}

#[pymethods]
impl PyProgressReporter {
    #[new]
    pub fn new(
        message: &str,
        total_unit_count: Option<usize>,
        total_byte_count: Option<usize>,
    ) -> Self {
        Self {
            dpr: DataProgressReporter::new(message, total_unit_count, total_byte_count),
        }
    }

    pub fn register_progress(&self, unit_amount: Option<usize>, bytes: Option<usize>) {
        self.dpr.register_progress(unit_amount, bytes)
    }

    pub fn update_target(&self, unit_delta_amount: Option<usize>, byte_delta: Option<usize>) {
        self.dpr.update_target(unit_delta_amount, byte_delta)
    }

    pub fn set_progress(&self, unit_amount: Option<usize>, bytes: Option<usize>) {
        self.dpr.set_progress(unit_amount, bytes)
    }

    pub fn finalize(&self) {
        self.dpr.finalize()
    }
}

/// This module is implemented in Rust.
#[pymodule]
pub fn rpyxet(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    let cfg = XetConfig::new(None, None, NoPath).unwrap();
    let _ = initialize_tracing_subscriber(&cfg);
    m.add_class::<PyRepoManager>()?;
    m.add_class::<PyRepo>()?;
    m.add_class::<PyWriteTransaction>()?;
    m.add_class::<PyWriteTransactionAccessToken>()?;
    m.add_class::<PyWFile>()?;
    m.add_class::<PyRFile>()?;
    m.add_class::<PyProgressReporter>()?;
    m.add_function(wrap_pyfunction!(configure_login, m)?)?;
    m.add_function(wrap_pyfunction!(perform_mount, m)?)?;
    m.add_function(wrap_pyfunction!(perform_mount_curdir, m)?)?;

    Ok(())
}
