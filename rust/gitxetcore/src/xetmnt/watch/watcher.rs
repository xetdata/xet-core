use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::info;

use crate::config::{UserSettings, XetConfig};
use crate::data_processing::PointerFileTranslator;
use crate::git_integration::git_wrap::get_git_executable;
use crate::log::ErrorPrinter;
use crate::xetmnt::watch::metadata::FSMetadata;

const REMOTE: &str = "origin";

pub struct RepoWatcher {
    fs: Arc<FSMetadata>,
    repo: Arc<Mutex<git2::Repository>>,
    pointer_translator: Arc<PointerFileTranslator>,
    xet_config: XetConfig,
    gitref: String,
    repo_dir: PathBuf,
}

impl RepoWatcher {
    pub fn new(
        fs: Arc<FSMetadata>,
        repo: Arc<Mutex<git2::Repository>>,
        pointer_translator: Arc<PointerFileTranslator>,
        xet_config: XetConfig,
        gitref: String,
        repo_dir: PathBuf,
    ) -> Self {
        Self {
            fs,
            repo,
            pointer_translator,
            xet_config,
            gitref,
            repo_dir,
        }
    }

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
            self.update_fs_to_commit(&repo, new_commit)?;
            info!("RepoWatcher: syncing notes to merkledb");
            self.refresh_mdb().await?;

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

    /// Refreshes the MerkleDB for the repo from the local notes by calling out to
    /// `git-xet merkledb extract-git`.
    ///
    /// It would be good to instead directly use the library functions (i.e. merge_merkledb_from_git),
    /// but that function contains objects that are not Send (namely, the Box<dyn Iterator>
    /// used to iterate over git notes), which prevents us from using it in a tokio::spawn task.
    async fn refresh_mdb(&self) -> Result<(), anyhow::Error> {
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

#[cfg(test)]
mod tests {
    use std::{env, io};
    use std::path::PathBuf;

    use git2::Commit;
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

    #[tokio::test]
    async fn test_repo_watcher() {
        setup_logging();
        let dir = "/var/folders/2m/brxjgbf52x5dgqj1wjdqw4hr0000gn/T/.tmpkVwGxq/repo";
        let gitref = "main";

        let repo_path = PathBuf::from(dir);
        let repo = git2::Repository::discover(&repo_path).unwrap();
        let config = XetConfig::new(None, None, ConfigGitPathOption::PathDiscover(repo_path.clone())).unwrap();
        let pfts = PointerFileTranslator::from_config(&config).await.unwrap();

        let oid = get_tree_oid(&repo, gitref);
        let fs = FSMetadata::new(&repo_path, oid).unwrap();

        let watcher = RepoWatcher::new(
            Arc::new(fs),
            Arc::new(Mutex::new(repo)),
            Arc::new(pfts),
            config,
            gitref.to_string(),
            repo_path,
        );

        watcher.run(Duration::from_secs(5)).await.unwrap();
    }
}
