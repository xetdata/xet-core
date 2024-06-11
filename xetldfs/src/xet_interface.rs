use crate::utils::resolve_path;
use lazy_static::lazy_static;
use libxet::config::XetConfig;
use libxet::data::{PointerFile, PointerFileTranslatorV2};
use libxet::errors::Result;
use libxet::git_integration::{resolve_repo_path, GitXetRepo};
use libxet::ErrorPrinter;
use openssl_probe;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, RwLock};
use std::{path::PathBuf, sync::Arc};
use tokio::runtime::Runtime;

lazy_static! {
    static ref TOKIO_RUNTIME: Arc<Runtime> = {
        let rt = Runtime::new().expect("Failed to create Tokio runtime");
        Arc::new(rt)
    };
    static ref XET_REPO_WRAPPERS: RwLock<Vec<Arc<XetFSRepoWrapper>>> = RwLock::new(Vec::new());
    static ref XET_ENVIRONMENT_CFG: Mutex<Option<XetConfig>> = Mutex::new(None);
}

fn get_base_config() -> Result<XetConfig> {
    // If the base config isn't set, then initialize everthing also..
    let mut cfg_wrap = XET_ENVIRONMENT_CFG.lock().unwrap();

    if cfg_wrap.is_none() {
        let cfg = XetConfig::new(None, None, libxet::config::ConfigGitPathOption::NoPath)?;

        libxet::environment::log::initialize_tracing_subscriber(&cfg)?;

        let _ = openssl_probe::try_init_ssl_cert_env_vars();

        eprintln!("CFG initialized.");

        *cfg_wrap = Some(cfg);
    }

    Ok(cfg_wrap.as_ref().unwrap().clone())
}

// Attempt to find all the instances.
pub fn get_xet_instance(raw_path: &str) -> Result<Option<Arc<XetFSRepoWrapper>>> {
    let path = resolve_path(raw_path)?;
    eprintln!("XetLDFS: get_xet_instance: {raw_path} resolved to {path:?}.");

    if let Some(repo_wrapper) = XET_REPO_WRAPPERS
        .read()
        .unwrap()
        .iter()
        .find(|xrw| path.starts_with(xrw.repo_path()))
        .map(|xfs| xfs.clone())
    {
        eprintln!("No xet instance found for {path:?} ( from {raw_path}");

        Ok(Some(repo_wrapper))
    } else {
        // See if we need to create it.
        let Some(start_path) = path.parent() else {
            return Ok(None);
        };

        // TODO: cache known directories as known non-xet paths.
        let Some(repo_path) = resolve_repo_path(Some(start_path.to_path_buf()), false)
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
        eprintln!("Repo path for {path:?}: {repo_path:?}");

        // Lock back here so we don't have multiple reads accessing the same repository
        let mut xet_repo_wrappers = XET_REPO_WRAPPERS.write().unwrap();

        // A check to make sure we're not opening multiple versions of this.
        for xrw in xet_repo_wrappers.iter() {
            if xrw.repo_path() == repo_path {
                return Ok(Some(xrw.clone()));
            }
        }

        let xet_repo = XetFSRepoWrapper::new(&repo_path)
            .map_err(|e| {
                eprintln!("Error occurred initializing repo wrapper from {repo_path:?}: {e:?}");
                e
            })
            .unwrap();

        xet_repo_wrappers.push(xet_repo.clone());

        Ok(Some(xet_repo))
    }
}

// Holds the data for the read information here.
// Like XetRFileObject but uses the full pointer file translator and stuff.
pub struct XetFSReadHandle {
    xet_fsw: Arc<XetFSRepoWrapper>,

    // TODO: flesh this out.
    pointer_file: PointerFile,
}

pub struct XetFSRepoWrapper {
    xet_repo: GitXetRepo,
    pft: Arc<PointerFileTranslatorV2>,
}

impl XetFSRepoWrapper {
    pub fn new(root_path: impl AsRef<Path>) -> Result<Arc<Self>> {
        let base_cfg = get_base_config()?;

        let cfg = base_cfg.switch_repo_path(
            libxet::config::ConfigGitPathOption::PathDiscover(root_path.as_ref().to_path_buf()),
            None,
        )?;

        let xrw = TOKIO_RUNTIME.handle().block_on(async move {
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

    pub fn open_path_for_read(&self, abs_path: impl AsRef<Path>) -> XetFSReadHandle {
        todo!();
    }

    pub fn materialize_path(&self, abs_path: impl AsRef<Path>) -> Result<()> {
        todo!();
    }
}
