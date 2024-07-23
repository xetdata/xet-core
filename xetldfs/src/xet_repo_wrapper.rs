use file_utils::SafeFileCreator;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use libxet::data::{PointerFile, PointerFileTranslatorV2};
use libxet::errors::Result;
use libxet::git_integration::GitXetRepo;

use crate::repo_path_utils::is_path_in_repo;
use crate::runtime::{self, tokio_run_error_checked, with_interposing_disabled};
use crate::xet_rfile::XetFdReadHandle;

/// Wrapper that lazily materializes a repository.
pub struct XetFSRepoWrapper {
    repo_path: PathBuf,
    repo: RwLock<Option<Arc<PointerFileTranslatorV2>>>,
}

impl XetFSRepoWrapper {
    pub fn new(repo_path: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            repo_path,
            repo: RwLock::new(None),
        })
    }

    pub fn file_in_repo(&self, file_path: impl AsRef<Path>) -> bool {
        is_path_in_repo(file_path, &self.repo_path)
    }

    pub fn get_read_handle_if_valid(&self, file_path: impl AsRef<Path>) -> Option<XetFdReadHandle> {
        let file_path = file_path.as_ref();
        let _lg = with_interposing_disabled();

        debug_assert!(self.file_in_repo(file_path));

        // Read the pointer file.
        let pointer_file = PointerFile::init_from_path(file_path);
        let is_valid = pointer_file.is_valid();

        // If it's valid, return a read handle for it.
        if is_valid {
            let Some(repo_pft) = self.get_pft() else {
                ld_error!(
                    "Error opening repo at {:?} to get read handle for file {file_path:?}.",
                    &self.repo_path
                );
                return None;
            };
            Some(XetFdReadHandle::new(repo_pft.clone(), pointer_file))
        } else {
            None
        }
    }

    pub fn ensure_path_materialized(&self, file_path: impl AsRef<Path>) -> bool {
        let file_path = file_path.as_ref();
        let _lg = with_interposing_disabled();

        let pf = PointerFile::init_from_path(file_path);

        if pf.is_valid() {
            let Some(repo_pft) = self.get_pft() else {
                ld_error!(
                    "Error opening repo at {:?} to materialize file {file_path:?}.",
                    &self.repo_path
                );
                return false;
            };

            tokio_run_error_checked(&format!("Materializing file {file_path:?}"), async move {
                let mut out_file = SafeFileCreator::replace_existing(file_path)?;

                repo_pft
                    .smudge_file_from_pointer(file_path, &pf, &mut out_file, None)
                    .await?;

                out_file.close()?;

                Result::Ok(())
            })
            .is_some()
        } else {
            false
        }
    }

    fn get_pft(&self) -> Option<Arc<PointerFileTranslatorV2>> {
        if let Some(t) = self.repo.read().unwrap().as_ref() {
            return Some(t.clone());
        }

        let mut init_lg = self.repo.write().unwrap();

        if let Some(t) = init_lg.as_ref() {
            return Some(t.clone());
        }

        let Some(base_cfg) = runtime::xet_cfg() else {
            ld_error!(
                "Error initializing repo at {:?}, cannot load config.",
                &self.repo_path
            );
            return None;
        };

        tokio_run_error_checked(
            &format!("Initializing repository at {:?}", &self.repo_path),
            async move {
                let xet_repo = GitXetRepo::open_at(base_cfg.clone(), self.repo_path.clone())?;

                let pft = Arc::new(
                    PointerFileTranslatorV2::from_config_smudge_only(&xet_repo.xet_config).await?,
                );

                *init_lg = Some(pft.clone());

                Result::Ok(pft)
            },
        )
    }
}
