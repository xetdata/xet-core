use anyhow::anyhow;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use git2::FetchOptions;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, info};

use crate::config::UserSettings;
use crate::log::ErrorPrinter;
use crate::xetmnt::watch::metadata::FSMetadata;

const REMOTE: &str = "origin";

struct GitCreds {
    username: String,
    token: String,
    ssh_name: String,
}

impl From<UserSettings> for GitCreds {
    fn from(value: UserSettings) -> Self {
        Self {
            username: value.name.unwrap_or_default(),
            token: value.token.unwrap_or_default(),
            ssh_name: value.ssh.unwrap_or_default(),
        }
    }
}

pub struct RepoWatcher {
    fs: Arc<FSMetadata>,
    repo: Arc<Mutex<git2::Repository>>,
    gitref: String,
    watch_interval: Duration,
    git_creds: Arc<GitCreds>,
}

impl RepoWatcher {
    pub fn new(
        fs: Arc<FSMetadata>,
        repo: Arc<Mutex<git2::Repository>>,
        gitref: String,
        watch_interval: Duration,
        user_settings: UserSettings,
    ) -> Self {
        Self {
            fs,
            repo,
            gitref,
            watch_interval,
            git_creds: Arc::new(user_settings.into()),
        }
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        loop {
            info!("RepoWatcher: start fetch");
            _ = self
                .fetch_from_remote()
                .await
                .log_error("RepoWatcher: error fetching from remote")
                .map(|_| info!("RepoWatcher: finished fetch"));
            sleep(self.watch_interval).await;
        }
    }

    pub async fn refresh(&self) -> Result<(), anyhow::Error> {
        info!("RepoWatcher: explicit refresh");
        self.fetch_from_remote()
            .await
            .map(|_| info!("RepoWatcher: finished refresh"))
    }

    /// Fetches our gitref from the remote repo, updating the FS root node if a change
    /// was found.  
    async fn fetch_from_remote(&self) -> Result<(), anyhow::Error> {
        let repo = self.repo.lock().await;
        let mut remote = repo.find_remote(REMOTE)?;

        info!(
            "RepoWatcher: download from remote: {}/{}",
            REMOTE, self.gitref
        );
        let cb = self.get_callback_for_fetch()?;
        let mut options = FetchOptions::new();
        options.remote_callbacks(cb);
        remote.download(&[&self.gitref], Some(&mut options))?;
        Self::log_fetch_stats(remote.stats());
        // Disconnect the underlying connection to prevent from idling.
        // We can't cache the connection since it is tied to the lifetime of the
        // locked repo. However, we might not actually need the lock on the repo.
        remote.disconnect()?;

        info!("RepoWatcher: updating local branch tips");
        // Update the references in the remote's namespace to point to the right
        // commits. This may be needed even if there was no packfile to download,
        // which can happen e.g. when the branches have been changed but all the
        // needed objects are available locally.
        let mut cb = git2::RemoteCallbacks::new();
        // This callback gets called for each remote-tracking branch that gets
        // updated. The message we output depends on whether it's a new one or an
        // update.

        let new_oid = Rc::new(RefCell::new(None));
        let new_oid2 = new_oid.clone();
        cb.update_tips(move |refname, a, b| {
            info!("RepoWatcher: found update for: {refname} ({a:10}..{b:10})");
            new_oid2.replace(Some(b));
            true
        });
        remote.update_tips(Some(&mut cb), true, git2::AutotagOption::Unspecified, None)?;
        if let Some(new_commit) = new_oid.borrow().as_ref() {
            let commit = repo.find_commit(*new_commit)?;
            let root_id = commit.tree_id();
            info!("RepoWatcher: Updating FS with new root_id: {root_id:?}");
            self.fs
                .update_root_oid(root_id)
                .map_err(|e| anyhow!("error updating FS to new rootID: {e:?}"))?;
        }
        Ok(())
    }

    /// Gets the git callbacks needed for the fetch command.
    /// This includes adding credentials to the remote and
    /// logging of output.
    fn get_callback_for_fetch(&self) -> Result<git2::RemoteCallbacks, anyhow::Error> {
        let mut cb = git2::RemoteCallbacks::new();
        self.set_log_callbacks(&mut cb);
        self.set_credentials_callback(&mut cb);
        Ok(cb)
    }

    /// Adds logging callbacks to print out git-related progress
    fn set_log_callbacks(&self, cb: &mut git2::RemoteCallbacks) {
        cb.sideband_progress(|data| {
            debug!("RepoWatcher: {}", String::from_utf8_lossy(data));
            true
        });
        // Here we show processed and total objects in the pack and the amount of
        // received data. Most frontends will probably want to show a percentage and
        // the download rate.
        cb.transfer_progress(|stats| {
            if stats.received_objects() == stats.total_objects() {
                debug!(
                    "RepoWatcher: Resolving deltas {}/{}",
                    stats.indexed_deltas(),
                    stats.total_deltas()
                );
            } else if stats.total_objects() > 0 {
                debug!(
                    "RepoWatcher: Received {}/{} objects ({}) in {} bytes",
                    stats.received_objects(),
                    stats.total_objects(),
                    stats.indexed_objects(),
                    stats.received_bytes()
                );
            }
            true
        });
    }

    /// Sets a callback to send the appropriate credentials back to the git command.
    fn set_credentials_callback(&self, cb: &mut git2::RemoteCallbacks) {
        let creds = self.git_creds.clone();
        cb.credentials(move |user, user_from_url, cred_type| {
            debug!(
            "Credentials callback: user: {user}, user_from_url: {user_from_url:?}, cred: {cred_type:?}"
        );
            match cred_type {
                git2::CredentialType::USER_PASS_PLAINTEXT => git2::Cred::userpass_plaintext(
                    &creds.username,
                    &creds.token,
                ),
                git2::CredentialType::SSH_KEY => {
                    git2::Cred::ssh_key_from_agent(&creds.ssh_name)
                }
                _ => {
                    let err = format!("credentials type: {cred_type:?} not supported");
                    Err(git2::Error::from_str(&err))
                }
            }
        });
    }

    fn log_fetch_stats(stats: git2::Progress) {
        if stats.total_objects() == 0 {
            // up to date
            return;
        }
        // we fetched new content
        if stats.local_objects() > 0 {
            info!(
                "RepoWatcher: Received {}/{} objects in {} bytes (used {} local objects)",
                stats.indexed_objects(),
                stats.total_objects(),
                stats.received_bytes(),
                stats.local_objects()
            );
        } else {
            info!(
                "RepoWatcher: Received {}/{} objects in {} bytes",
                stats.indexed_objects(),
                stats.total_objects(),
                stats.received_bytes()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::{env, io};

    use git2::Commit;
    use tracing::Level;
    use tracing_subscriber::fmt::writer::MakeWriterExt;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

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
        let dir = "/var/folders/2m/brxjgbf52x5dgqj1wjdqw4hr0000gn/T/.tmpdBbzBo/repo";
        let gitref = "main";

        let repo_path = PathBuf::from(dir);
        let repo = git2::Repository::discover(&repo_path).unwrap();

        let oid = get_tree_oid(&repo, gitref);
        let fs = FSMetadata::new(&repo_path, oid).unwrap();

        let watcher = RepoWatcher::new(
            Arc::new(fs),
            Arc::new(Mutex::new(repo)),
            gitref.to_string(),
            Duration::from_secs(5),
            UserSettings {
                name: Some("jgodlew".to_string()),
                token: Some(env::var("XETHUB_PAT_DEV").unwrap()),
                ssh: Some("jgodlew".to_string()),
                ..Default::default()
            },
        );

        watcher.run().await.unwrap();
    }
}
