use crate::path_utils::{path_of_fd, resolve_path};
use crate::runtime::{with_interposing_disabled, TOKIO_RUNTIME};
use crate::xet_rfile::{close_fd_if_registered, XetFdReadHandle};
use file_utils::SafeFileCreator;
use lazy_static::lazy_static;
use libc::mode_t;
use libxet::config::XetConfig;
use libxet::data::{PointerFile, PointerFileTranslatorV2};
use libxet::errors::Result;
use libxet::git_integration::{get_repo_path, GitXetRepo};
use libxet::ErrorPrinter;
use openssl_probe;
use std::path::{Component, Path};
use std::sync::RwLock;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Mutex as TMutex;

use crate::{cstring_to_str, interposed_close, my_close, real_close, runtime};
use crate::{my_dup2, real_open, ENABLE_CALL_TRACING};

use crate::{ld_trace, ld_warn};

lazy_static! {
    static ref XET_REPO_WRAPPERS: RwLock<Vec<Arc<XetFSRepoWrapper>>> = RwLock::new(Vec::new());
    static ref XET_ENVIRONMENT_CFG: TMutex<Option<XetConfig>> = TMutex::new(None);
}

// Requires runnig inside tokio runtime, so async
async fn get_base_config() -> Result<XetConfig> {
    // If the base config isn't set, then initialize everthing also..
    let mut cfg_wrap = XET_ENVIRONMENT_CFG.lock().await;

    if cfg_wrap.is_none() {
        let cfg = XetConfig::new(None, None, libxet::config::ConfigGitPathOption::NoPath)?;

        libxet::environment::log::initialize_tracing_subscriber(&cfg)?;

        let _ = openssl_probe::try_init_ssl_cert_env_vars();

        ld_trace!("CFG initialized.");

        *cfg_wrap = Some(cfg);
    }

    Ok(cfg_wrap.as_ref().unwrap().clone())
}

// Attempt to find all the instances.
pub fn get_repo_context(raw_path: &str) -> Result<Option<(Arc<XetFSRepoWrapper>, PathBuf)>> {
    let _ig = with_interposing_disabled();

    ld_trace!("get_repo_context: {raw_path}");
    let path = resolve_path(raw_path).map_err(|e| {
        ld_trace!("resolve_path failed: {e:?}");
        e
    })?;
    ld_trace!("get_repo_context: {raw_path} resolved to {path:?}");

    if path
        .components()
        .any(|c| matches!(c, Component::Normal(name) if name == ".git"))
    {
        return Ok(None);
    }

    ld_trace!("get_repo_context: {raw_path} is not inside .git");

    // quick failure without trying opening **and implicitly setup** a repo.
    let pf = PointerFile::init_from_path(&path);
    ld_trace!("get_repo_context: pointer file is {pf:?}");
    if !pf.is_valid() {
        ld_trace!("get_repo_context: {raw_path} is not a valid pointer file");
        return Ok(None);
    }

    ld_trace!("get_repo_context: {raw_path} is a pointer file");

    if let Some(repo_wrapper) = XET_REPO_WRAPPERS
        .read()
        .unwrap()
        .iter()
        .find(|xrw| path.starts_with(xrw.repo_path()))
        .map(|xfs| xfs.clone())
    {
        ld_trace!("Xet instance found for {path:?} ( from {raw_path}");

        return Ok(Some((repo_wrapper, path)));
    }

    // See if we need to create it.
    let Some(start_path) = path.parent() else {
        return Ok(None);
    };

    // TODO: cache known directories as known non-xet paths.
    let Some(repo_path) = get_repo_path(Some(start_path.to_path_buf()))
        .map_err(|e| {
            eprintln!("Error Initializing repo from {start_path:?} : {e:?}");
            e
        })
        .unwrap_or(None)
    else {
        eprintln!("No repo path found for {start_path:?}");
        return Ok(None);
    };

    // TODO: Do more than print that we have this.
    ld_trace!("Repo path for {path:?}: {repo_path:?}");

    // Lock back here so we don't have multiple reads accessing the same repository
    let mut xet_repo_wrappers = XET_REPO_WRAPPERS.write().unwrap();

    // Check within the lock to make sure we're not opening multiple versions of this.
    for xrw in xet_repo_wrappers.iter() {
        if xrw.repo_path() == repo_path {
            return Ok(Some((xrw.clone(), path)));
        }
    }

    let xet_repo = XetFSRepoWrapper::new(&repo_path)
        .map_err(|e| {
            ld_trace!("Error occurred initializing repo wrapper from {repo_path:?}: {e:?}");
            e
        })
        .unwrap();

    xet_repo_wrappers.push(xet_repo.clone());

    Ok(Some((xet_repo, path)))
}

pub struct XetFSRepoWrapper {
    pub xet_repo: GitXetRepo,
    pub pft: Arc<PointerFileTranslatorV2>,
}

impl XetFSRepoWrapper {
    pub fn new(root_path: impl AsRef<Path>) -> Result<Arc<Self>> {
        let xrw = TOKIO_RUNTIME.handle().block_on(async move {
            let base_cfg = get_base_config().await?;

            let cfg = base_cfg.switch_repo_path(
                libxet::config::ConfigGitPathOption::PathDiscover(root_path.as_ref().to_path_buf()),
                None,
            )?;

            let xet_repo = GitXetRepo::open_and_verify_setup(cfg).await?;
            let pft = Arc::new(
                PointerFileTranslatorV2::from_config_smudge_only(&xet_repo.xet_config).await?,
            );
            Result::Ok(Self { pft, xet_repo })
        })?;

        Ok(Arc::new(xrw))
    }

    pub fn repo_path(&self) -> &Path {
        &self.xet_repo.repo_dir
    }

    pub async fn open_path_for_read_if_pointer(
        self: &Arc<Self>,
        path: PathBuf,
    ) -> Result<Option<XetFdReadHandle>> {
        let pf = PointerFile::init_from_path(&path);

        if !pf.is_valid() {
            Ok(None)
        } else {
            Ok(Some(XetFdReadHandle::new(self.clone(), pf)))
        }
    }

    pub async fn materialize_path(&self, abs_path: impl AsRef<Path>) -> Result<()> {
        let pf = PointerFile::init_from_path(&abs_path);

        let mut out_file = SafeFileCreator::replace_existing(&abs_path)?;

        self.pft
            .smudge_file_from_pointer(abs_path.as_ref(), &pf, &mut out_file, None)
            .await?;

        out_file.close()?;

        Ok(())
    }
}

pub fn materialize_file_under_fd_if_needed(fd: libc::c_int) -> bool {
    unsafe {
        let _ig = with_interposing_disabled();

        // Convert the file descriptor to a file path
        if let Some(path) = path_of_fd(fd) {
            // Materialize the file if it's a pointer file
            if materialize_rw_file_if_needed(cstring_to_str(&path)) {
                let flags = libc::fcntl(fd, libc::F_GETFL);
                if flags == -1 {
                    ld_warn!(
                        "materialize_file_under_fd: Error retrieving flags for orginial fd={fd}."
                    );
                    return false;
                }

                // Get the original file's mode
                let file_mode = libc::fcntl(fd, libc::F_GETFD);
                if file_mode == -1 {
                    ld_warn!(
                        "materialize_file_under_fd: Error retrieving mode for orginial fd={fd}."
                    );
                    return false;
                }

                let new_fd = real_open(path.as_ptr(), flags, file_mode as mode_t);

                if new_fd == -1 {
                    ld_warn!("materialize_file_under_fd: Error opening materialized file at {path:?} : {:?}", 
                        std::io::Error::last_os_error());
                    return false;
                }

                let dup2_res = my_dup2(new_fd, fd);

                if dup2_res == -1 {
                    ld_warn!(
                        "materialize_file_under_fd: Error calling dup2 to replace old path: {:?}",
                        std::io::Error::last_os_error()
                    );
                    return false;
                }

                real_close(new_fd);

                return true;
            }
        }
    }
    false
}

pub fn materialize_rw_file_if_needed(path: &str) -> bool {
    if let Ok(Some((xet_repo, path))) = get_repo_context(path).map_err(|e| {
        eprintln!("Error in get_repo_context for materializing {path}: {e:?}");
        e
    }) {
        TOKIO_RUNTIME.handle().block_on(async move {
            let _ = xet_repo
                .materialize_path(path)
                .await
                .log_error("Error Materializing path={path:?}");
        });
        true
    } else {
        false
    }
}
