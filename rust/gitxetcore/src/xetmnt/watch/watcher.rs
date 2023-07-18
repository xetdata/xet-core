use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::info;

use crate::data_processing::PointerFileTranslator;
use crate::git_integration::git_wrap::get_git_executable;
use crate::log::ErrorPrinter;
use crate::xetmnt::watch::metadata::FSMetadata;

const REMOTE: &str = "origin";

pub struct RepoWatcher {
    fs: Arc<FSMetadata>,
    repo: Arc<Mutex<git2::Repository>>,
    pointer_translator: Arc<PointerFileTranslator>,
    gitref: String,
    repo_dir: PathBuf,
}

impl RepoWatcher {
    pub fn new(
        fs: Arc<FSMetadata>,
        repo: Arc<Mutex<git2::Repository>>,
        pointer_translator: Arc<PointerFileTranslator>,
        gitref: String,
        repo_dir: PathBuf,
    ) -> Self {
        Self {
            fs,
            repo,
            pointer_translator,
            gitref,
            repo_dir,
        }
    }

    #[allow(unused)]
    pub async fn refresh(&self) -> Result<(), anyhow::Error> {
        info!("RepoWatcher: explicit refresh");
        self.fetch_from_remote()
            .await
            .map(|_| info!("RepoWatcher: finished refresh"))
    }

    pub async fn run(&self, interval: Duration) -> Result<(), anyhow::Error> {
        loop {
            info!("RepoWatcher: start fetch");
            _ = self
                .fetch_from_remote()
                .await
                .log_error("RepoWatcher: error fetching from remote")
                .map(|_| info!("RepoWatcher: finished fetch"));
            sleep(interval).await;
        }
    }

    /// Fetches our gitref from the remote repo, updating the FS root node if a change
    /// was found.  
    async fn fetch_from_remote(&self) -> Result<(), anyhow::Error> {
        let repo = self.repo.lock().await;
        let maybe_new_commit = self.sync_remote(&repo)?;

        if let Some(new_commit) = maybe_new_commit {
            info!("RepoWatcher: new commit found: ({new_commit:?})");
            info!("RepoWatcher: syncing notes to merkledb");
            self.refresh_mdb().await?;
            info!("RepoWatcher: resetting the FS metadata");
            self.update_fs_to_commit(&repo, new_commit)?;

            info!("RepoWatcher: Reloading merkledb into pointer file translator");
            self.pointer_translator.reload_mdb().await;
        }
        Ok(())
    }

    /// Perform git operations to fetch all refs and check if the oid for [self.gitref]
    /// changed. If it has changed, then Ok(Some(new_oid)) is returned.
    fn sync_remote(&self, repo: &git2::Repository) -> Result<Option<git2::Oid>, anyhow::Error> {
        let old_oid = self.get_oid_for_ref(repo)?;

        let mut remote = repo.find_remote(REMOTE)?;
        remote.fetch(&[] as &[&str], None, None)?;

        let new_oid = self.get_oid_for_ref(repo)?;
        Ok((old_oid != new_oid).then_some(new_oid))
    }

    pub fn get_oid_for_ref(&self, repo: &git2::Repository) -> Result<git2::Oid, anyhow::Error> {
        let gitref = &self.gitref;
        let rev = repo.revparse_single(gitref)?;
        rev.as_commit()
            .map(git2::Commit::id)
            .ok_or(anyhow!("expecting reference {gitref} to point to a commit"))
    }

    fn update_fs_to_commit(
        &self,
        repo: &git2::Repository,
        new_commit: git2::Oid,
    ) -> Result<(), anyhow::Error> {
        let commit = repo.find_commit(new_commit)?;
        let root_id = commit.tree_id();
        info!("RepoWatcher: Updating FS with new root_id: {root_id:?}");
        self.fs
            .update_root_oid(root_id)
            .map_err(|e| anyhow!("error updating FS to new rootID: {e:?}"))?;
        Ok(())
    }

    /// Refreshes the on-disk MerkleDB for the repo from the local git notes.
    ///
    /// Currently, this is done by calling out to `git-xet merkledb extract-git`.
    ///
    /// It would be good to instead directly use the library functions (i.e. merge_merkledb_from_git),
    /// but that function contains objects that are not Send (namely, the Box<dyn Iterator>
    /// used to iterate over git notes), which prevents us from using it in a tokio::spawn task
    /// (i.e. using RepoWatcher as a background task).
    ///
    /// Note that tokio::task::local_spawn doesn't work unless we change startup of the git-xet
    /// application to use a LocalSet, which has implications on parallelism.
    async fn refresh_mdb(&self) -> Result<(), anyhow::Error> {
        // TODO: use this once we can make GitRepo Send.
        // let repo = git_repo::GitRepo::open(self.xet_config.clone())?;
        // Ok(repo.sync_notes_to_dbs().await?)

        let mut command = if let Ok(curexe) = std::env::current_exe() {
            Command::new(curexe)
        } else {
            let mut command = Command::new(get_git_executable());
            command.arg("xet");
            command
        };
        command.current_dir(&self.repo_dir);
        command.arg("merkledb");
        command.arg("extract-git");
        command
            .status()
            .map_err(|e| anyhow!("error refreshing merkledb: {e:?}"))?;
        info!("updated merkledb.db");
        Ok(())
    }
}

#[allow(dead_code)]
#[allow(unused_imports)]
#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    use git2::{Commit, Repository, Signature};
    use tempfile::TempDir;
    use tracing::Level;
    use tracing_subscriber::fmt::writer::MakeWriterExt;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    use crate::config::{ConfigGitPathOption, XetConfig};

    use super::*;

    fn setup_logging() {
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_line_number(true)
            .with_file(true)
            .with_target(false)
            .compact()
            .with_writer(io::stdout.with_max_level(Level::INFO));

        tracing_subscriber::registry().with(fmt_layer).init();
    }

    fn get_tree_oid(repo: &git2::Repository, gitref: &str) -> git2::Oid {
        let rev = repo.revparse_single(gitref).unwrap();
        rev.as_commit().map(Commit::tree_id).unwrap()
    }

    // TODO: this is an example way to run the watcher against some repo and manually
    //       validate that refreshing is activating properly. Would like to automate
    //       this test / remove the hardcoded repo dependencies.
    // #[tokio::test]
    // async fn test_repo_watcher() {
    //     setup_logging();
    //     let dir = "/var/folders/2m/brxjgbf52x5dgqj1wjdqw4hr0000gn/T/.tmpkVwGxq/repo";
    //     let gitref = "main";
    //
    //     let repo_path = PathBuf::from(dir);
    //     let repo = git2::Repository::discover(&repo_path).unwrap();
    //     let config = XetConfig::new(None, None, ConfigGitPathOption::PathDiscover(repo_path.clone())).unwrap();
    //     let pfts = PointerFileTranslator::from_config(&config).await.unwrap();
    //
    //     let oid = get_tree_oid(&repo, gitref);
    //     let fs = FSMetadata::new(&repo_path, oid).unwrap();
    //
    //     let watcher = RepoWatcher::new(
    //         Arc::new(fs),
    //         Arc::new(Mutex::new(repo)),
    //         Arc::new(pfts),
    //         gitref.to_string(),
    //         repo_path,
    //     );
    //
    //     watcher.run(Duration::from_secs(5)).await.unwrap();
    // }
}
