use is_executable::IsExecutable;
use mdb_shard::error::MDBShardError;
use mdb_shard::shard_version::ShardVersion;
use std::collections::{HashMap, HashSet};
use std::fs::create_dir_all;
#[cfg(unix)]
use std::fs::Permissions;
use std::fs::{self, File, OpenOptions};
#[cfg(unix)]
use std::os::unix::prelude::PermissionsExt;

use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use std::sync::Arc;

use crate::config::ConfigGitPathOption;
use crate::config::XetConfig;
use git2::Repository;
use lazy_static::lazy_static;
use regex::Regex;
use tracing::{debug, error, info, warn};

use crate::constants::*;
use crate::data_processing::PointerFileTranslator;
use crate::errors::GitXetRepoError::{self};
use crate::errors::Result;
use crate::git_integration::git_wrap;
use crate::merkledb_plumb::{
    self, check_merklememdb_is_empty, merge_merkledb_from_git, update_merkledb_to_git,
};
use crate::merkledb_shard_plumb::{self, get_mdb_version};
use crate::summaries_plumb::{merge_summaries_from_git, update_summaries_to_git};

use super::git_notes_wrapper::GitNotesWrapper;
use super::git_url::{authenticate_remote_url, is_remote_url, parse_remote_url};

// For each reference update that was added to the transaction, the hook receives
// on standard input a line of the format:
//
//    <old-value> SP <new-value> SP <ref-name> LF
//
// where <old-value> is the old object name passed into the reference transaction,
// <new-value> is the new object name to be stored in the ref and <ref-name> is
// the full name of the ref.
lazy_static! {
    static ref REF_REGEX: Regex =
        Regex::new(r"^[^ ]+ +[^ ]+ +refs/remotes/([^/]+)/?([^ ]*)$").unwrap();
}

fn reference_transaction_hook_regex() -> &'static Regex {
    &REF_REGEX
}

///////////////////////////
// Git attributes.
const GITATTRIBUTES_CONTENT: &str =
    "* filter=xet diff=xet merge=xet -text\n*.gitattributes filter=\n*.xet/** filter=\n";
lazy_static! {
    static ref GITATTRIBUTES_TEST_REGEX: Regex = Regex::new(
        r"(^\* filter=xet.* -text$)|(^\*\.gitattributes filter=$)|(^\*\.xet/\*\* filter=$)"
    )
    .unwrap();
}

const PREPUSH_HOOK_CONTENT: &str =
    "git-xet hooks pre-push-hook --remote \"$1\" --remote-loc \"$2\"\n";
const REFERENCE_TRANSACTION_HOOK_CONTENT: &str =
    "git-xet hooks reference-transaction-hook --action \"$1\"\n";

// Provides a mechanism to lock files that can often be modifiied, such as hooks,
// .gitattributes, etc.  Normally our mechanisms should handle all these files
// automatically, but this provides a way to force files to never change.  Adding
// in the comment text:
//
// # XET LOCK
//
// On any line in the file, and the file will never change.
//
lazy_static! {
    static ref CONTENT_LOCKING_REGEX: Regex = Regex::new(r".*XET +LOCK.*").unwrap();
}
/// Returns true if the locking text `# XET LOCK` appears in the text, and false otherwise.
fn file_content_contains_lock(content: &str) -> bool {
    CONTENT_LOCKING_REGEX.is_match(content)
}

pub fn is_user_identity_set(path: Option<PathBuf>) -> std::result::Result<bool, git2::Error> {
    let (_, username, _) =
        git_wrap::run_git_captured(path.as_ref(), "config", &["user.name"], false, None).map_err(
            |e| git2::Error::from_str(&format!("Error retrieving config setting user.name: {e:?}")),
        )?;

    let (_, email, _) =
        git_wrap::run_git_captured(path.as_ref(), "config", &["user.email"], false, None).map_err(
            |e| {
                git2::Error::from_str(&format!(
                    "Error retrieving config setting user.email: {e:?}"
                ))
            },
        )?;

    Ok(!(username.trim().is_empty() || email.trim().is_empty()))
}

pub fn verify_user_config(path: Option<PathBuf>) -> std::result::Result<(), git2::Error> {
    if !is_user_identity_set(path)? {
        return Err(git2::Error::from_str(
            "Configure your Git user name and email before running git-xet commands. \
\n\n  git config --global user.name \"<Name>\"\n  git config --global user.email \"<Email>\"",
        ));
    }

    Ok(())
}

// Map from MDB version to ref notes canonical name
pub fn get_merkledb_notes_name(version: &ShardVersion) -> &'static str {
    match version {
        ShardVersion::V1 => GIT_NOTES_MERKLEDB_V1_REF_NAME,
        ShardVersion::V2 => GIT_NOTES_MERKLEDB_V2_REF_NAME,
        &ShardVersion::Uninitialized => "",
    }
}

/// Open the repo using libgit2
pub fn open_libgit2_repo(
    repo_path: Option<&Path>,
) -> std::result::Result<Arc<Repository>, git2::Error> {
    let repo = match repo_path {
        Some(path) => Repository::discover(path)?,
        None => Repository::open_from_env()?,
    };

    #[allow(unknown_lints)]
    #[allow(clippy::arc_with_non_send_sync)]
    Ok(Arc::new(repo))
}

// Salt is 256-bit in length.
pub const REPO_SALT_LEN: usize = 32;

// Read one blob from the notesref as salt.
// Return error if find more than one note.
pub fn read_repo_salt(git_dir: &Path) -> Result<Option<[u8; REPO_SALT_LEN]>> {
    let notesref = GIT_NOTES_REPO_SALT_REF_NAME;

    let Ok(repo) = open_libgit2_repo(Some(git_dir)).map_err(|e| {
        info!("Error opening {git_dir:?} as git repository; error = {e:?}.");
        e
    }) else {
        return Ok(None);
    };

    if repo.find_reference(notesref).is_err() {
        info!("Repository at {git_dir:?} does not appear to contain {notesref}, salt not found.");
        return Ok(None);
    }

    let notes_wrapper = GitNotesWrapper::from_repo(repo, notesref);
    let mut iter = notes_wrapper.notes_content_iterator()?;
    let Some((_, salt_data)) = iter.next() else {
        info!("Error reading repo salt from notes: {notesref} present but empty.");
        return Ok(None);
    };

    if salt_data.len() != REPO_SALT_LEN {
        return Err(GitXetRepoError::Other(format!(
            "mismatch repo salt length from notes: {:?}",
            salt_data.len()
        )));
    }

    if iter.count() != 0 {
        return Err(GitXetRepoError::Other(
            "find more than one repo salt".to_owned(),
        ));
    }

    let mut ret = [0u8; REPO_SALT_LEN];
    ret.copy_from_slice(&salt_data);

    Ok(Some(ret))
}

pub struct GitRepo {
    #[allow(dead_code)]
    pub repo: Arc<Repository>,
    xet_config: XetConfig,
    pub repo_dir: PathBuf,
    pub git_dir: PathBuf,
    pub mdb_version: ShardVersion,
    pub merkledb_file: PathBuf,
    pub merkledb_v2_cache_dir: PathBuf,
    pub merkledb_v2_session_dir: PathBuf,
    pub summaries_file: PathBuf,
    pub cas_staging_path: PathBuf,
}

impl GitRepo {
    /// loads the current repository
    fn load_repo(repo_dir: Option<&Path>) -> std::result::Result<Arc<Repository>, git2::Error> {
        match repo_dir {
            Some(path) => {
                if *path == PathBuf::default() {
                    open_libgit2_repo(None)
                } else {
                    open_libgit2_repo(Some(path))
                }
            }
            None => open_libgit2_repo(None),
        }
    }

    fn repo_dir_from_repo(repo: &Arc<Repository>) -> PathBuf {
        match repo.workdir() {
            Some(p) => p,
            None => repo.path(), // When it's a bare directory
        }
        .to_path_buf()
    }

    pub fn open(config: XetConfig) -> Result<Self> {
        Self::open_impl(config, false)
    }

    pub fn open_and_initialize(config: XetConfig) -> Result<Self> {
        Self::open_impl(config, true)
    }

    /// Open the repository, assuming that the current directory is itself in the repository.
    ///
    /// If we are running in a way that is not associated with a repo, then the XetConfig path
    /// will be
    fn open_impl(config: XetConfig, initialize_if_uninitialized: bool) -> Result<Self> {
        let repo = Self::load_repo(Some(config.repo_path()?))?;

        let git_dir = repo.path().to_path_buf();
        let repo_dir = Self::repo_dir_from_repo(&repo);
        info!(
            "GitRepo::open: Opening git repo at {:?}, git_dir = {:?}.",
            repo_dir, git_dir
        );

        let merkledb_file = {
            if config.merkledb == PathBuf::default() {
                git_dir.join(MERKLEDBV1_PATH_SUBDIR)
            } else {
                config.merkledb.clone()
            }
        };

        let merkledb_v2_cache_dir = {
            if config.merkledb_v2_cache == PathBuf::default() {
                git_dir.join(MERKLEDB_V2_CACHE_PATH_SUBDIR)
            } else {
                config.merkledb_v2_cache.clone()
            }
        };

        let merkledb_v2_session_dir = {
            if config.merkledb_v2_session == PathBuf::default() {
                git_dir.join(MERKLEDB_V2_SESSION_PATH_SUBDIR)
            } else {
                config.merkledb_v2_session.clone()
            }
        };

        let summaries_file = {
            if config.summarydb == PathBuf::default() {
                git_dir.join(SUMMARIES_PATH_SUBDIR)
            } else {
                config.summarydb.clone()
            }
        };

        let cas_staging_path = {
            let stage_path_or_default = config.staging_path.clone().unwrap_or_default();
            if stage_path_or_default == PathBuf::default() {
                git_dir.join(CAS_STAGING_SUBDIR)
            } else {
                stage_path_or_default
            }
        };

        // Now, see what version we're at in this repo and whether it's initialized or not.
        let mut mdb_version = get_mdb_version(&repo_dir)?;

        let mut s = Self {
            repo,
            git_dir,
            repo_dir,
            xet_config: config,
            mdb_version,
            merkledb_file,
            merkledb_v2_cache_dir,
            merkledb_v2_session_dir,
            summaries_file,
            cas_staging_path,
        };

        if mdb_version == ShardVersion::Uninitialized {
            info!("GitRepo::open: Detected repo is not initialized (ShardVersion::Unitialized)");

            // First, attempt a fetch from the remote notes, which may actually have more information than
            // we explicitly have at this point.  The filter will run on clone before the remote notes
            // have been fetched, so in this case we need to explicitly fetch them.
            let remotes = Self::list_remote_names(s.repo.clone())?;
            for r in remotes {
                s.sync_remote_to_notes(&r)?;
            }
            s.sync_note_refs_to_local("reposalt", GIT_NOTES_REPO_SALT_REF_SUFFIX)?;
            mdb_version = get_mdb_version(&s.repo_dir)?;

            // If it's still unitialized, and we are told to initialize it, then go for it.
            if mdb_version == ShardVersion::Uninitialized && initialize_if_uninitialized {
                s.install_gitxet_from_filter_process()?;

                s.mdb_version = get_mdb_version(&s.repo_dir)?;

                if s.mdb_version != ShardVersion::V2 {
                    error!("GitRepo::open: Error Initializing new repo.");
                    return Err(GitXetRepoError::Other(
                        "Error: Setting MerkleDB shard version failed on implicit initialization."
                            .to_owned(),
                    ));
                }
            }
        }

        Ok(s)
    }

    /// Clone a repo -- just a pass-through to git clone.
    /// Return repo name and a branch field if that exists in the remote url.
    pub fn clone(
        config: Option<&XetConfig>,
        git_args: &[&str],
        no_smudge: bool,
        base_dir: Option<&PathBuf>,
        pass_through: bool,
        check_result: bool,
    ) -> Result<(String, Option<String>)> {
        let mut git_args = git_args.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        // attempt to rewrite URLs with authentication information
        // if config provided

        let mut repo = String::default();
        let mut branch = None;
        if let Some(config) = config {
            for ent in &mut git_args {
                if is_remote_url(ent) {
                    (*ent, repo, branch) = parse_remote_url(ent)?;
                    *ent = authenticate_remote_url(ent, config)?;
                }
            }
        }
        if let Some(ref br) = branch {
            git_args.extend(vec!["--branch".to_owned(), br.clone()]);
        }

        // First, make sure that everything is properly installed and that the git-xet filter will be run correctly.
        Self::write_global_xet_config()?;

        let smudge_arg: Option<&[_]> = if no_smudge {
            Some(&[("XET_NO_SMUDGE", "1")])
        } else {
            None
        };
        let git_args_ref: Vec<&str> = git_args.iter().map(|s| s.as_ref()).collect();

        // Now run git clone, and everything should work fine.
        if pass_through {
            git_wrap::run_git_passthrough(
                base_dir,
                "clone",
                &git_args_ref,
                check_result,
                smudge_arg,
            )?;
        } else {
            git_wrap::run_git_captured(base_dir, "clone", &git_args_ref, check_result, smudge_arg)?;
        }

        Ok((repo, branch))
    }

    pub fn get_remote_urls(path: Option<&Path>) -> Result<Vec<String>> {
        let repo = Self::load_repo(path)?;
        // try to derive from git repo URL
        // get the list of remotes
        Ok(Self::list_remotes(&repo)?
            .into_iter()
            .map(|(_name, url)| url)
            .collect())
    }

    pub fn get_remote_names() -> Result<Vec<String>> {
        let repo = Self::load_repo(None)?;
        // try to derive from git repo URL
        // get the list of remotes
        Self::list_remote_names(repo)
    }

    /// Calls git directly, capturing stderr and returning the result of stdout.
    ///
    /// The command is run in the directory base_directory.  On nonzero exit
    /// status, an error is return containing the captured stderr output.
    pub fn run_git(&self, command: &str, args: &[&str]) -> Result<(Option<i32>, String, String)> {
        git_wrap::run_git_captured(None, command, args, false, None)
    }

    pub fn run_git_checked(&self, command: &str, args: &[&str]) -> Result<String> {
        let (_, stdout_s, _) = git_wrap::run_git_captured(None, command, args, true, None)?;
        Ok(stdout_s)
    }

    pub fn run_git_in_repo(
        &self,
        command: &str,
        args: &[&str],
    ) -> Result<(Option<i32>, String, String)> {
        git_wrap::run_git_captured(Some(&self.repo_dir), command, args, false, None)
    }

    pub fn run_git_checked_in_repo(&self, command: &str, args: &[&str]) -> Result<String> {
        let (_, stdout_s, _) =
            git_wrap::run_git_captured(Some(&self.repo_dir), command, args, true, None)?;
        Ok(stdout_s)
    }

    /// Returns true if the current repo is clean, and false otherwise.
    pub fn repo_is_clean(&self) -> Result<bool> {
        // Here, we'll run the fast query version.  However, this relies on
        // the existance of HEAD, which isn't always the case.
        let (ret_code, _, _) = self.run_git_in_repo("update-index", &["--refresh"])?;

        if let Some(r) = ret_code {
            if r != 0 {
                return Ok(false);
            }
        }

        let (ret_code, _, _) = self.run_git_in_repo("diff-index", &["--quiet", "HEAD", "--"])?;

        Ok(match ret_code {
            Some(0) => true,
            Some(_) => {
                //
                // Here we need to determine whether it's a new repo or not,
                // as this causes the process to falsely report it's dirty.  This
                // command will list out the current changes.
                let (_, stdout, _) =
                    self.run_git_in_repo("status", &["--porcelain", "--untracked-files=no"])?;
                stdout.is_empty() // If there are no commits, then it's clean
            }
            _ => {
                return Err(GitXetRepoError::Other(
                    "git subprocesses killed unexpectedly.".to_string(),
                ))
            }
        })
    }

    /// Writing out all the initial configuration files and results.
    ///
    /// May be run multiple times without changing the current configuration..
    pub async fn install_gitxet(
        &mut self,
        global_config: bool,
        always_write_local_config: bool,
        preserve_gitattributes: bool,
        force: bool,
        enable_locking: bool,
        mdb_version: u64,
    ) -> Result<bool> {
        info!("Running install associated with repo {:?}", self.repo_dir);

        let mdb_version = ShardVersion::try_from(mdb_version)?;

        if !self.repo_is_clean()? {
            return Err(GitXetRepoError::Other("Repository must be clean to run git-xet init.  Please commit or stash any changes and rerun.".to_owned()));
        }

        if !force {
            let remotes = GitRepo::list_remotes(&self.repo)
                .map_err(|_| GitXetRepoError::Other("Unable to list remotes".to_string()))?;

            let mut have_ok_remote = false;

            for (r_name, r_url) in remotes {
                for endpoint in XET_ALLOWED_ENDPOINTS {
                    if r_url.contains(endpoint) {
                        have_ok_remote = true;
                        info!(
                            "Remote {} with endpoint {} is a XET remote; allowing init.",
                            &r_name, &r_url
                        );
                        break;
                    } else {
                        info!(
                            "Excluded remote {}; endpoint {} is not a XET remote.",
                            &r_name, &r_url
                        );
                    }
                }
            }
            if !have_ok_remote {
                return Err(GitXetRepoError::Other("No registered XetData remote; aborting initialization.  Use --force to override.".to_owned()));
            }
        }

        if global_config {
            GitRepo::write_global_xet_config()?;
        } else if always_write_local_config {
            self.write_local_filter_config()?;
        } else {
            // Do we need to write the local config?  If it's in the global config,
            let (_, filter_value, _) =
                self.run_git_in_repo("config", &["--global", "filter.xet.process"])?;
            if filter_value.trim() != "git xet filter" {
                self.write_local_filter_config()?;
            }
        }

        let (changed, git_attr_changed) = self
            .initialize(!preserve_gitattributes, enable_locking)
            .await?;

        // If the git attributes file is changed, then commit that change so it's pushed properly.
        if git_attr_changed {
            self.run_git_checked_in_repo(
                "add",
                &[self.repo_dir.join(".gitattributes").to_str().unwrap()],
            )?;
            self.run_git_checked("commit", &["-m", "Configured repository to use git-xet."])?;
        }

        self.set_repo_mdb(&mdb_version).await?;

        if mdb_version.need_salt() {
            self.set_repo_salt()?;
        }

        Ok(changed)
    }

    /// Set up a bare repo with xet specific information.
    ///
    /// May be run multiple times without changing the current configuration.
    pub async fn install_gitxet_for_bare_repo(&self, mdb_version: u64) -> Result<()> {
        info!(
            "Configuring Merkle DB and repo salt associated with repo {:?}",
            self.repo_dir
        );

        let mdb_version = ShardVersion::try_from(mdb_version)?;
        self.set_repo_mdb(&mdb_version).await?;

        if mdb_version.need_salt() {
            self.set_repo_salt()?;
        }

        Ok(())
    }

    /// Set up a repo when the filter is run but no notes are present.
    /// This can happen on certain
    ///
    /// May be run multiple times without changing the current configuration.
    pub fn install_gitxet_from_filter_process(&self) -> Result<()> {
        // Initialize a local version of the
        info!(
            "Configure Merkle DB and repo salt associated with repo {:?}",
            &self.repo_dir
        );

        self.set_repo_mdb_to_v2_from_uninitialized()?;

        Ok(())
    }

    // Write out all the initial configurations and hooks.
    //
    // Returns two flags.  The first is true if any changes were made,
    // the second is true if parts of the local config were changed
    // and should be committed after this.

    pub async fn initialize(
        &mut self,
        overwrite_gitattributes: bool,
        enable_locking: bool,
    ) -> Result<(bool, bool)> {
        let mut changed = false;

        // Go through and ensure everything is as it should be.
        changed |= self.create_directories()?;
        changed |= self.write_prepush_hook()?;
        changed |= self.write_reference_transaction_hook()?;

        if enable_locking {
            // Test for the existence of git lfs.
            if let Err(e) = self.run_git_checked("lfs", &["version"]) {
                info!("Error string attempting to query for git lfs: {e:?}");
                return Err(GitXetRepoError::InvalidOperation("Error: git lfs not found; required to enable locking (from --enable-locking flag).".to_string()));
            }

            changed |= self.write_postcheckout_hook()?;
            changed |= self.write_postmerge_hook()?;
            changed |= self.write_postcommit_hook()?;
        }

        let git_attr_changed = self.write_gitattributes(overwrite_gitattributes)?;

        changed |= git_attr_changed;

        // This is a passive thing; may not be needed.
        let new_remotes = self.write_repo_fetch_config()?;

        // TODO: this may need a timeout or something; it's possible that
        // things from these remotes fail or hang due to connection issues.
        if !new_remotes.is_empty() {
            for remote in &new_remotes {
                self.sync_remote_to_notes(remote)?;
            }
            self.mdb_version = get_mdb_version(&self.git_dir)?;
            self.sync_notes_to_dbs().await?;
        }

        Ok((changed || !new_remotes.is_empty(), git_attr_changed))
    }

    pub fn create_directories(&self) -> Result<bool> {
        let mut changed = false;

        let cas_dir = &self.cas_staging_path;
        let merkledb_dir = &self.merkledb_file.parent().unwrap().to_path_buf();
        let merkledb_v2_cache_dir = &self.merkledb_v2_cache_dir;
        let merkledb_v2_session_dir = &self.merkledb_v2_session_dir;

        info!(
            "XET: Ensuring directories exist: {:?}, {:?}, {:?}, {:?}",
            &merkledb_dir, &cas_dir, &merkledb_v2_cache_dir, &merkledb_v2_session_dir,
        );

        for dir in [
            &cas_dir,
            &merkledb_dir,
            &merkledb_v2_cache_dir,
            &merkledb_v2_session_dir,
        ] {
            if !dir.exists() {
                changed = true;
                create_dir_all(dir)?;
                info!("XET: Created dir {:?}.", &dir);
            }
        }

        Ok(changed)
    }

    /// Writes out the .gitattributes, or (possibly) modifies it if it's already present.
    pub fn write_gitattributes(&self, force_write_gitattributes: bool) -> Result<bool> {
        // Make sure that * filter=xet is in .gitattributes.

        let text = GITATTRIBUTES_CONTENT;

        let path = self.repo_dir.join(".gitattributes");

        if path.exists() {
            let content = fs::read(&path).unwrap_or_default();
            let content = std::str::from_utf8(&content).unwrap_or_default();

            // See if there is a lock flag in the .gitattributes file.
            if file_content_contains_lock(content) {
                info!(".gitattributes file contains locking tag, refusing to alter.");

                if force_write_gitattributes {
                    eprintln!("ERROR: Cannot change .gitattributes; file content is locked.  To update the file, remove the line containing \"XET LOCK\" and rerun the operation.");
                    error!("Cannot change .gitattributes; file content is locked.  To update the file, remove the line containing \"XET LOCK\" and rerun the operation.");
                }

                return Ok(false);
            }

            // Check to make sure all the relevant lines are there and at the beginning.
            if !content.starts_with(text) {
                if !force_write_gitattributes {
                    // See if it's actually containing content.
                    if !content
                        .lines()
                        .take(GITATTRIBUTES_CONTENT.matches('\n').count())
                        .all(|line| GITATTRIBUTES_TEST_REGEX.is_match(line.trim()))
                    {
                        warn!(".gitattributes file written, but contains non-xet content.");
                        eprintln!("WARNING: .gitattributes file written, but contains non-xet content.  To update this file, run `git xet init` to force writing out the file.  To silence this warning, add the comment \"# XET LOCK\" at the top of the .gitattributes file.");
                    }
                    return Ok(false);
                }

                info!("XET: modifying .gitattributes to include filter file.");

                // Go through line by line and see if the filter is present.
                // If so, then add this to the top but keep the rest.

                let mut new_loc = self.repo_dir.join(".gitattributes.bk");

                if new_loc.exists() {
                    for i in 0.. {
                        new_loc = self.repo_dir.join(&format!(".gitattributes.bk.{i:?}"));

                        if !new_loc.exists() {
                            break;
                        }
                    }
                }
                eprintln!(
                    "Modifying .gitattributes to verify filter process; moving old version to {:?}",
                    new_loc.file_name().unwrap()
                );

                fs::rename(&path, &new_loc)?;

                let mut out = File::create(&path)?;
                out.write_all(text.as_bytes())?;
                out.write_all("\n".as_bytes())?;

                // Put the rest of the lines there, in proper order.
                let verification_check: HashSet<&str> = text
                    .split('\n')
                    .filter(|s| {
                        let ss = s.trim();
                        !ss.is_empty()
                    })
                    .collect();

                for line in content.split('\n') {
                    if !verification_check.contains(&line) {
                        out.write_all(line.as_bytes())?;
                        out.write_all("\n".as_bytes())?;
                    }
                }
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            info!("XET: writing .gitattributes.");
            fs::write(&path, text)?;
            Ok(true)
        }
    }

    #[cfg(unix)]
    fn set_execute_permission(path: &Path) -> Result<()> {
        fs::set_permissions(path, Permissions::from_mode(0o755))?;
        Ok(())
    }

    #[cfg(windows)]
    fn set_execute_permission(_path: &Path) -> Result<()> {
        // do nothing because Windows FS doesn't have a concept of executable
        Ok(())
    }

    fn write_hook(&self, subpath: &str, script: &str) -> Result<bool> {
        let path = self.git_dir.join(subpath);

        let mut changed = false;

        // There are three states to handle:
        // 1. the file doesn't exist -> write out a new script
        // 2. the file exists and contains the script -> do nothing
        // 3. the file exists and doesn't contain script -> append
        if !path.exists() {
            let parent_path = path
                .parent()
                .ok_or_else(|| GitXetRepoError::FileNotFound(path.clone()))?;

            fs::create_dir_all(parent_path)?;

            info!("XET: writing {}.", &subpath);
            fs::write(&path, format!("#!/usr/bin/env bash\n{script}"))?;
            changed = true;
        } else {
            let content = fs::read(&path).unwrap_or_default();
            let content = std::str::from_utf8(&content).unwrap_or_default();

            if file_content_contains_lock(content) {
                info!("Hook {:?} contains locking text, not modifying.", subpath);
                return Ok(false);
            }

            if !content.contains(script) {
                let mut file = OpenOptions::new().append(true).open(&path)?;
                writeln!(file, "{script}")?;
                changed = true;
                info!("Adding hooks to file {:?}, appending to end.", subpath);
            }
        }

        // Make sure the executable status is set.
        if !path.is_executable() {
            Self::set_execute_permission(&path)?;
            changed = true;
        }

        Ok(changed)
    }

    /// Write out the prepush hook.
    pub fn write_prepush_hook(&self) -> Result<bool> {
        self.write_hook("hooks/pre-push", PREPUSH_HOOK_CONTENT)
    }

    /// Write out the post-merge hook
    pub fn write_postmerge_hook(&self) -> Result<bool> {
        let script = "git-xet hooks post-merge-hook --flag \"$1\"\n";
        self.write_hook("hooks/post-merge", script)
    }

    pub fn write_postcheckout_hook(&self) -> Result<bool> {
        let script =
            "git-xet hooks post-checkout-hook --previous \"$1\" --new \"$2\" --flag \"$3\"\n";
        self.write_hook("hooks/post-checkout", script)
    }

    pub fn write_postcommit_hook(&self) -> Result<bool> {
        let script = "git-xet hooks post-commit-hook\n";
        self.write_hook("hooks/post-commit", script)
    }

    /// Write out the reference transaction hook.
    pub fn write_reference_transaction_hook(&self) -> Result<bool> {
        self.write_hook(
            "hooks/reference-transaction",
            REFERENCE_TRANSACTION_HOOK_CONTENT,
        )
    }

    /// Uninstall all the local hooks from the current repository
    pub fn uninstall_gitxet_hooks(&self, ignore_locks: bool) -> Result<bool> {
        let mut all_uninstalled_correctly = true;
        eprintln!("Uninstalling git-xet hooks from repository.");

        let mut hook_erase_content: HashSet<&str> = HashSet::new();

        for hook_line in &[PREPUSH_HOOK_CONTENT, REFERENCE_TRANSACTION_HOOK_CONTENT] {
            for s in hook_line
                .lines()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            {
                hook_erase_content.insert(s);
            }
        }

        // Clear out the hooks.
        for hook in &["hooks/pre-push", "hooks/reference-transaction"] {
            let path = self.git_dir.join(hook);
            let content = match fs::read(&path) {
                Ok(s) => s,
                Err(_) => {
                    error!(
                        "Unable to open {:?} to clear git-xet hooks; skipping.",
                        &path
                    );
                    all_uninstalled_correctly = false;
                    continue;
                }
            };

            let content = match std::str::from_utf8(&content) {
                Err(e) => {
                    error!(
                        "Unable to parse contents of {:?} to clear git-xet hooks; skipping. (Error: {:?})",
                        &path, &e
                    );
                    all_uninstalled_correctly = false;
                    continue;
                }
                Ok(content) => content,
            };

            if !ignore_locks && file_content_contains_lock(content) {
                error!("Skipping {:?} in uninstall due to file locking.  Either rerun with --ignore-locks, or remove # XET LOCK from the file.", &path);
                all_uninstalled_correctly = false;
                continue;
            }

            let lines: Vec<&str> = content
                .lines()
                .filter(|s| !hook_erase_content.contains(s.trim()))
                .collect();
            let filtered_lines: Vec<&str> = lines
                .iter()
                .filter(|s| !s.trim().is_empty())
                .copied()
                .collect();

            // If there is nothing there but the bash script, then remove the file.
            if filtered_lines.is_empty()
                || (filtered_lines.len() == 1 && filtered_lines[0].starts_with("#!"))
            {
                if let Err(e) = fs::remove_file(&path) {
                    error!(
                        "Error attempting to remove hook file at {:?}, skipping (Error={:?})",
                        &path, &e
                    );
                    all_uninstalled_correctly = false;
                    continue;
                }
                info!("Removed git-xet hook at {:?}", &path);
            } else {
                if let Err(e) = fs::write(&path, lines.join("\n")) {
                    error!(
                        "Error attempting to rewrite hook file at {:?}, skipping (Error={:?})",
                        &path, &e
                    );
                    all_uninstalled_correctly = false;
                    continue;
                }
                info!("Cleared git-xet hook call from {:?}", &hook);
            }
        }

        Ok(all_uninstalled_correctly)
    }

    /// Purge git-xet filters from local .gitattributes file.
    #[allow(clippy::collapsible_else_if)]
    pub fn purge_gitattributes(&self, ignore_locks: bool) -> Result<(bool, bool)> {
        eprintln!("Purging git-xet hooks from .gitattributes file.");
        let path = self.repo_dir.join(".gitattributes");

        // Just so break; skips the rest.
        let content = match fs::read(&path) {
            Err(e) => {
                error!(
                    "Unable to parse content from {:?}; skipping (Error = {:?})",
                    &path, &e
                );
                return Ok((false, false));
            }
            Ok(c) => c,
        };

        let content = match std::str::from_utf8(&content) {
            Err(e) => {
                error!(
                    "Unable to parse content from {:?}; skipping. (Error = {:?}",
                    &path, &e
                );
                return Ok((false, false));
            }
            Ok(c) => c,
        };

        if !ignore_locks && file_content_contains_lock(content) {
            error!("Skipping {:?} in uninstall due to file locking.  Either rerun with --ignore-locks, or remove # XET LOCK from the file.", &path);
            return Ok((false, false));
        }

        let lines: Vec<&str> = content
            .lines()
            .filter(|s| !GITATTRIBUTES_TEST_REGEX.is_match(s.trim()))
            .collect();

        if !lines.iter().any(|s| !s.trim().is_empty()) {
            // Go ahead and remove it.
            info!("Removing empty .gitattributes");
            if let Err(e) = fs::remove_file(&path) {
                error!(
                    "Error attempting to remove .gitattributes file at {:?}, skipping (Error={:?})",
                    &path, &e
                );
                Ok((false, false))
            } else {
                info!("Removed .gitattributes file.");
                Ok((true, true))
            }
        } else {
            if let Err(e) = fs::write(&path, lines.join("\n")) {
                error!(
                        "Error attempting to rewrite .gitattributes file at {:?} without git-xet content, skipping (Error={:?})",
                        &path, &e
                    );
                Ok((false, false))
            } else {
                info!("Cleared git-xet content from .gitattributes file.");
                Ok((true, false))
            }
        }
    }

    pub fn remove_local_gitxet_configuration(&self) {}

    /// Uninstalls gitxet from the local repository.
    ///
    pub fn remove_components_from_local_repo(
        &self,
        args: &crate::command::uninit::UninitArgs,
    ) -> Result<()> {
        info!("XET: Uninstalling git-xet.");

        if !self.repo_is_clean()? {
            return Err(GitXetRepoError::Other("Repository must be clean to uninstall xet from local repository.  Please run with --global-only or commit or stash any changes and rerun.".to_owned()));
        }

        let mut all_uninstalled_correctly = true;
        let mut removed_paths: Vec<&str> = Vec::new();
        let mut modified_paths: Vec<&str> = Vec::new();

        if args.remove_hooks || args.full {
            all_uninstalled_correctly &= self.uninstall_gitxet_hooks(args.ignore_locks)?;
        }

        if args.purge_gitattributes || args.full {
            let (uninstalled_correctly, gitattr_removed) =
                self.purge_gitattributes(args.ignore_locks)?;
            all_uninstalled_correctly &= uninstalled_correctly;
            if uninstalled_correctly {
                if gitattr_removed {
                    removed_paths.push(".gitattributes");
                } else {
                    modified_paths.push(".gitattributes");
                }
            }
        }

        if args.purge_xet_config || args.full {
            eprintln!("Cleaning data folders .xet and .git/xet from repository.");
            info!("Removing .xet/ directory.");
            let config_dir = self.repo_dir.join(".xet");
            if config_dir.exists() {
                if let Err(e) = std::fs::remove_dir_all(&config_dir) {
                    error!(
                        "Error attempting to remove .xet/ directory, skipping (Error={:?})",
                        &e
                    );
                    all_uninstalled_correctly = false;
                } else {
                    info!("Cleared .xet/ config directory.");
                    removed_paths.push(".xet");
                }
            }
        }

        if args.purge_xet_data_dir || args.full {
            info!("Removing .git/xet directory.");
            if let Err(e) = std::fs::remove_dir_all(self.repo_dir.join(".git/xet")) {
                error!(
                    "Error attempting to remove .git/xet/ directory, skipping (Error={:?})",
                    &e
                );
                all_uninstalled_correctly = false;
            }
        }

        if args.purge_filter_config || args.full {
            self.purge_local_filter_config()?;
        }

        if args.purge_fetch_config || args.full {
            self.purge_local_fetch_config()?;
        }

        if (!removed_paths.is_empty() || !modified_paths.is_empty()) && !self.repo_is_clean()? {
            // Check on repo being clean in case this was run twice and parts are already committed.
            for file in removed_paths {
                self.run_git_checked_in_repo("rm", &[file])?;
            }
            for file in modified_paths {
                self.run_git_checked_in_repo("add", &[file])?;
            }

            self.run_git_checked(
                "commit",
                &["-m", "Uninstalled git-xet components from repository."],
            )?;
        }

        if all_uninstalled_correctly {
            eprintln!("Successfully uninstalled Xet components from repository.");
        }

        Ok(())
    }

    pub fn current_remotes(&self) -> Result<Vec<String>> {
        info!("XET: Listing git remote names");

        // first get the list of remotes. this version
        let remotes = self.repo.remotes()?;

        let mut result = Vec::new();

        // get the remote object and extract the URLs
        let mut i = remotes.iter();
        while let Some(Some(remote)) = i.next() {
            result.push(remote.to_string());
        }
        Ok(result)
    }

    pub fn list_remotes(repo: &Repository) -> Result<Vec<(String, String)>> {
        info!("XET: Listing git remotes");

        // first get the list of remotes
        let remotes = match repo.remotes() {
            Err(e) => {
                error!("Error: Unable to list remotes : {:?}", &e);
                return Ok(vec![]);
            }
            Ok(r) => r,
        };

        if remotes.is_empty() {
            return Ok(vec![]);
        }

        let mut result = Vec::new();
        // get the remote object and extract the URLs
        let mut i = remotes.iter();
        while let Some(Some(remote)) = i.next() {
            if let Some(info) = repo.find_remote(remote)?.url() {
                result.push((remote.to_string(), info.to_string()));
            }
        }
        Ok(result)
    }

    /// List all the remote names in a repo
    pub fn list_remote_names(repo: Arc<Repository>) -> Result<Vec<String>> {
        info!("XET: Listing git remotes");

        // first get the list of remotes
        let remotes = repo.remotes()?;

        // get the remote object and extract the URLs
        Ok(remotes.into_iter().flatten().map(String::from).collect())
    }

    // Writes out remote fetch information for the given remote,
    // or the current remote if None.

    pub fn write_repo_fetch_config(&self) -> Result<Vec<String>> {
        let mut changed_fetch_configs: Vec<String> = Vec::new();

        // Fetch the current config that matches a given pattern, saving the output.
        // If we parse it, then we can determine whether things should change.

        // If a new remote was added -- i.e. one of the remotes does not have a matching
        // tracking note config -- then we trigger a remote fetch to pull those notes.  The
        // reference transaction hook should also catch it, but may not right away.
        let (_, config_settings, _) = self.run_git_in_repo(
            "config",
            &["--get-regex", "remote\\.[a-z]+\\.fetch", ".*/notes/xet/.*"],
        )?;

        let repo_fetch_heads: HashMap<&str, &str> = config_settings
            .split('\n')
            .map(|line| line.split_once(' '))
            .filter_map(|e| e.map(|vv| (vv.0.trim(), vv.1.trim())))
            .collect();

        for remote in self.current_remotes()? {
            let config_name = format!("remote.{}.fetch", &remote);
            let config_value = format!("+refs/notes/xet/*:refs/remotes/{}/notes/xet/*", &remote);

            if let Some(v) = repo_fetch_heads.get(config_name.as_str()) {
                if *v == config_value {
                    debug!("XET: Fetch hooks on remote.{}.fetch is set.", &remote);
                    continue;
                }
            }
            info!("XET: Setting fetch hooks on remote.{}.fetch.", &remote);

            self.run_git_checked_in_repo("config", &["--add", &config_name, &config_value])?;
            changed_fetch_configs.push(remote);
        }

        Ok(changed_fetch_configs)
    }

    pub fn purge_local_fetch_config(&self) -> Result<bool> {
        info!("XET: Purging fetch hooks on local config.");

        // Get all the specific fetch settings.
        let (_, config_settings, _) = self.run_git_in_repo(
            "config",
            &[
                "--local",
                "--get-regex",
                "remote\\.[a-z]+\\.fetch",
                ".*/notes/xet/.*",
            ],
        )?;

        let repo_fetch_values: Vec<&str> = config_settings
            .split('\n')
            .map(|line| line.split_once(' '))
            .filter_map(|e| e.map(|vv| vv.0.trim()))
            .collect();

        for repo in repo_fetch_values {
            self.run_git_checked_in_repo(
                "config",
                &[
                    "--local",
                    "--unset-all",
                    &format!("remote.{repo}.fetch"),
                    ".*/notes/xet/.*",
                ],
            )?;
        }

        Ok(true)
    }

    /// Write out the filter config to global settings.
    pub fn write_global_xet_config() -> Result<()> {
        info!("XET: Setting global filter config.");

        git_wrap::run_git_captured(
            None,
            "config",
            &["--global", "filter.xet.process", "git xet filter"],
            true,
            None,
        )?;

        git_wrap::run_git_captured(
            None,
            "config",
            &["--global", "--bool", "filter.xet.required", "true"],
            true,
            None,
        )?;

        Ok(())
    }

    /// Purge the filter config from global settings.
    pub fn purge_global_filter_config() -> Result<()> {
        info!("XET: Unsetting global filter config.");

        git_wrap::run_git_captured(
            None,
            "config",
            &["--global", "--unset-all", "filter.xet.process"],
            true,
            None,
        )?;

        git_wrap::run_git_captured(
            None,
            "config",
            &["--global", "--unset-all", "filter.xet.required"],
            true,
            None,
        )?;
        Ok(())
    }

    /// Write the filter config to local repository settings.
    pub fn write_local_filter_config(&self) -> Result<()> {
        info!("XET: Setting local filter config.");
        self.run_git_checked_in_repo(
            "config",
            &["--local", "filter.xet.process", "git xet filter"],
        )?;
        self.run_git_checked_in_repo(
            "config",
            &["--local", "--bool", "filter.xet.required", "true"],
        )?;

        Ok(())
    }

    /// Purge the filter config from local repository settings.
    pub fn purge_local_filter_config(&self) -> Result<()> {
        info!("XET: Setting local filter config.");
        self.run_git_in_repo("config", &["--local", "--unset-all", "filter.xet.process"])?;
        self.run_git_in_repo("config", &["--local", "--unset-all", "filter.xet.required"])?;

        Ok(())
    }

    /// Adds any changes in the database files to the notes.
    pub async fn sync_dbs_to_notes(&self) -> Result<()> {
        info!("XET sync_dbs_to_notes: syncing merkledb to git notes.");
        match self.mdb_version {
            ShardVersion::V1 => {
                update_merkledb_to_git(
                    &self.xet_config,
                    &self.merkledb_file,
                    GIT_NOTES_MERKLEDB_V1_REF_NAME,
                )
                .await?
            }
            ShardVersion::V2 => {
                merkledb_shard_plumb::sync_mdb_shards_to_git(
                    &self.xet_config,
                    &self.merkledb_v2_session_dir,
                    &self.merkledb_v2_cache_dir,
                    GIT_NOTES_MERKLEDB_V2_REF_NAME,
                )
                .await?
            }
            ShardVersion::Uninitialized => {
                error!("sync_dbs_to_notes: Error, repo not initialized yet.");
                return Err(GitXetRepoError::RepoUninitialized(
                    "Attempted sync_dbs_to_notes when repo is not initialized for git xet use."
                        .to_owned(),
                ));
            }
        }

        info!("XET sync_dbs_to_notes: syncing summaries to git notes.");
        update_summaries_to_git(
            &self.xet_config,
            &self.summaries_file,
            GIT_NOTES_SUMMARIES_REF_NAME,
        )
        .await?;

        Ok(())
    }

    /// Sync all the notes to the Merkle DB.
    pub async fn sync_notes_to_dbs(&self) -> Result<()> {
        info!("XET sync_notes_to_dbs.");

        self.sync_note_refs_to_local("merkledb", GIT_NOTES_MERKLEDB_V1_REF_SUFFIX)?;
        self.sync_note_refs_to_local("merkledbv2", GIT_NOTES_MERKLEDB_V2_REF_SUFFIX)?;
        self.sync_note_refs_to_local("reposalt", GIT_NOTES_REPO_SALT_REF_SUFFIX)?;
        self.sync_note_refs_to_local("summaries", GIT_NOTES_SUMMARIES_REF_SUFFIX)?;

        debug!("XET sync_notes_to_dbs: merging MDB");
        match self.mdb_version {
            ShardVersion::V1 => {
                merge_merkledb_from_git(
                    &self.xet_config,
                    &self.merkledb_file,
                    GIT_NOTES_MERKLEDB_V1_REF_NAME,
                )
                .await?
            }
            ShardVersion::V2 => {
                merkledb_shard_plumb::sync_mdb_shards_from_git(
                    &self.xet_config,
                    &self.merkledb_v2_cache_dir,
                    GIT_NOTES_MERKLEDB_V2_REF_NAME,
                    true, // with Shard client we can disable this in the future
                )
                .await?
            }
            ShardVersion::Uninitialized => {
                debug!("sync_notes_to_dbs: skipping due to ShardVersion::Unitialized");
            }
        }

        debug!("XET sync_notes_to_dbs: merging summaries");
        merge_summaries_from_git(
            &self.xet_config,
            &self.summaries_file,
            GIT_NOTES_SUMMARIES_REF_NAME,
        )
        .await?;

        Ok(())
    }

    /// Sync minimal notes to Merkle DB for Xetblob operations
    pub async fn sync_notes_to_dbs_for_xetblob(&self) -> Result<()> {
        info!("XET sync_notes_to_dbs_for_xetblob.");

        debug!("XET sync_notes_to_dbs_for_xetblob: merging MDB");
        if self.mdb_version == ShardVersion::V1 {
            merge_merkledb_from_git(
                &self.xet_config,
                &self.merkledb_file,
                GIT_NOTES_MERKLEDB_V1_REF_NAME,
            )
            .await?
        }

        Ok(())
    }

    /// Syncronizes any fetched note refs to the local notes
    pub fn sync_note_refs_to_local(&self, note_suffix: &str, notes_ref_suffix: &str) -> Result<()> {
        for xet_p in ["xet", "xet_alt"] {
            let ref_suffix = format!("notes/{}/{}", &xet_p, note_suffix);

            let (_, remote_refs, _) = self.run_git_in_repo("show-ref", &["--", &ref_suffix])?;

            for hash_ref in remote_refs.split('\n') {
                if hash_ref.is_empty() {
                    continue;
                }

                let split_v: Vec<&str> = hash_ref.split(' ').collect();
                debug_assert_eq!(split_v.len(), 2);
                let remote_ref = &split_v[0];
                let ref_name = &split_v[1];

                if !ref_name.starts_with("refs/remotes/") {
                    debug!("skipping non-remote ref {}", &ref_name);
                    continue;
                }

                info!("XET sync_note_refs_to_local: updating {}", &ref_name);

                if !remote_ref.is_empty() {
                    self.run_git_checked_in_repo(
                        "notes",
                        &[&format!("--ref={notes_ref_suffix}"), "merge", remote_ref],
                    )?;
                }
            }
        }

        Ok(())
    }

    /// Syncs the remote notes to the local notes
    pub fn sync_remote_to_notes(&self, remote: &str) -> Result<()> {
        info!("XET sync_remote_to_notes: remote = {}", &remote);

        // The empty --refmap= argument is needed to avoid triggering automatic
        // fetch in remote.origin.fetch option.
        // fetching to xet_alt is needed to avoid conflicts with the
        // remote.origin.fetch process fetching to notes/xet/

        self.run_git_checked_in_repo(
            "fetch",
            &[
                remote,
                "--refmap=",
                "--no-write-fetch-head",
                &format!("+refs/notes/xet/*:refs/remotes/{}/notes/xet_alt/*", &remote),
            ],
        )?;

        self.sync_note_refs_to_local("merkledb", GIT_NOTES_MERKLEDB_V1_REF_SUFFIX)?;
        self.sync_note_refs_to_local("merkledbv2", GIT_NOTES_MERKLEDB_V2_REF_SUFFIX)?;
        self.sync_note_refs_to_local("summaries", GIT_NOTES_SUMMARIES_REF_SUFFIX)?;

        Ok(())
    }

    /// Sync minimal remote notes to local for Xetblob operations
    pub fn sync_remote_to_notes_for_xetblob(&self, remote: &str) -> Result<()> {
        info!("XET sync_remote_to_notes_for_xetblob: remote = {}", &remote);

        self.run_git_checked_in_repo(
            "fetch",
            &[
                remote,
                "--refmap=",
                "--no-write-fetch-head",
                "+refs/notes/xet/merkledb*:refs/notes/xet/merkledb*",
            ],
        )?;

        Ok(())
    }

    /// Sync all the notes containing the MDB to the remote.
    pub fn sync_notes_to_remote(&self, remote: &str) -> Result<()> {
        info!("XET sync_notes_to_remote: remote = {}", &remote);

        self.sync_remote_to_notes(remote)?;

        match self.mdb_version {
            ShardVersion::V1 => self.run_git_checked_in_repo(
                "push",
                &[
                    "--no-verify",
                    remote,
                    GIT_NOTES_MERKLEDB_V1_REF_NAME,
                    GIT_NOTES_SUMMARIES_REF_NAME,
                ],
            )?,
            ShardVersion::V2 | ShardVersion::Uninitialized => {
                self.run_git_checked_in_repo("push", &["--no-verify", remote, "refs/notes/xet/*"])?
            }
        };

        Ok(())
    }

    /// Pushes all the staged data in the local CAS
    pub async fn upload_all_staged(&self) -> Result<()> {
        let repo = PointerFileTranslator::from_config(&self.xet_config).await?;
        repo.upload_cas_staged(false).await?;
        Ok(())
    }

    /// The pre-push hook
    pub async fn pre_push_hook(&self, remote: &str) -> Result<()> {
        info!("Running prepush hook with remote = {}", remote);

        // upload all staged should start first
        // in case the other db has issues, we are guaranteed to at least
        // get the bytes off the machine before anything else gets actually
        // pushed

        // the first upload staged is to ensure all xorbs are synced
        // so shard registration (in MDBv2) in sync_dbs_to_notes will find them.
        self.upload_all_staged().await?;
        self.sync_dbs_to_notes().await?;
        // the second upload staged is to ensure xorbs associated with large MDBv1
        // diff as standalone pointer file as synced.
        self.upload_all_staged().await?;
        self.sync_notes_to_remote(remote)?;

        Ok(())
    }

    /// The lfs post merge hook
    pub async fn post_merge_lfs_hook(&self, flag: &str) -> Result<()> {
        info!("Running post-merge hook");
        use std::process::Command;

        Command::new("git")
            .args(["lfs", "post-merge", flag])
            .output()?;
        Ok(())
    }

    /// The lfs post checkout hook
    pub async fn post_checkout_lfs_hook(
        &self,
        previous: &str,
        new: &str,
        flag: &str,
    ) -> Result<()> {
        info!("Running post-checkout hook");

        use std::process::Command;

        Command::new("git")
            .args(["lfs", "post-checkout", previous, new, flag])
            .output()?;
        Ok(())
    }

    /// The lfs post commit hook
    pub async fn post_commit_lfs_hook(&self) -> Result<()> {
        info!("Running post-commit hook");

        use std::process::Command;

        Command::new("git").args(["lfs", "post-commit"]).output()?;
        Ok(())
    }

    pub fn reference_transaction_hook(&self, action: &str) -> Result<()> {
        // TODO: if the action isn't what we want, do we have to read all of stdin?
        debug!(
            "XET reference_transaction_hook: called with action = {}",
            &action
        );

        if action != "committed" {
            return Ok(());
        }

        info!("XET reference_transaction_hook: running.");

        let mut input: String = String::new();

        let re = reference_transaction_hook_regex();

        loop {
            input.clear();

            //
            let n_read = std::io::stdin().read_line(&mut input)?;

            // If the function above returns Ok(0), the stream has reached EOF.
            if n_read == 0 {
                debug!("XET reference_transaction_hook: input EOF detected.");
                break;
            }

            debug!(
                "XET reference_transaction_hook: read line \"{}\" with {} bytes.",
                &input, &n_read
            );

            let captured_text = match re.captures(&input) {
                Some(m) => m,
                None => {
                    debug!("XET reference_transaction_hook: match failed; skipping.");
                    continue;
                }
            };

            debug!(
                "XET reference_transaction_hook: Regex match on ref: {}",
                captured_text.get(0).unwrap().as_str()
            );

            let remote = captured_text.get(1).unwrap().as_str();

            // Now, does this contain notes?
            if let Some(m) = captured_text.get(2) {
                if m.as_str().contains("notes") {
                    debug!(
                             "XET reference_transaction_hook: update contains notes reference; skipping."
                        );

                    continue;
                }
            }
            // only do a sync if the remote exists
            if let Ok(remotenames) = Self::get_remote_names() {
                if remotenames.iter().any(|x| x == remote) {
                    debug!("XET reference_transaction_hook: Found matching remote. Syncing.");
                    self.sync_remote_to_notes(remote)?;
                }
            }
        }

        Ok(())
    }

    async fn check_merkledb_is_empty(&self, version: &ShardVersion) -> Result<bool> {
        let notesref = get_merkledb_notes_name(version);
        match version {
            ShardVersion::V1 => {
                check_merklememdb_is_empty(&self.xet_config, &self.merkledb_file, notesref)
                    .await
                    .map_err(GitXetRepoError::from)
            }
            ShardVersion::V2 | ShardVersion::Uninitialized => todo!(), // should never get here
        }
    }

    fn set_repo_mdb_to_v2_from_uninitialized(&self) -> Result<()> {
        // Only need to install guard notes when this is an upgrade.

        merkledb_shard_plumb::write_mdb_version_guard_note(
            &self.repo_dir,
            get_merkledb_notes_name,
            &ShardVersion::V2,
        )?;

        // Also adds a note with empty data, this ensures the particular ref notes
        // exists so git doesn't report error on push. Git blob store ensures that
        // only one copy is stored.
        merkledb_shard_plumb::add_empty_note(
            &self.xet_config,
            get_merkledb_notes_name(&ShardVersion::V2),
        )?;

        Ok(())
    }

    async fn set_repo_mdb(&self, version: &ShardVersion) -> Result<()> {
        if self.mdb_version > *version {
            return Err(MDBShardError::ShardVersionError(format!(
                "illegal to downgrade Merkle DB from {:?} to {version:?}",
                self.mdb_version
            )))
            .map_err(GitXetRepoError::from);
        }

        // Only need to install guard notes when this is an upgrade.
        if self.mdb_version < *version && self.mdb_version != ShardVersion::Uninitialized {
            // Make sure Merkle DB is empty before set verison.
            let mut v = *version;
            while let Some(lower_version) = v.get_lower() {
                if !self.check_merkledb_is_empty(&lower_version).await? {
                    return Err(MDBShardError::ShardVersionError(format!(
                    "failed to set Merkle DB version to {version:?} because Merkle DB is not empty"
                )))
                    .map_err(GitXetRepoError::from);
                }
                v = lower_version;
            }

            info!(
                "Resetting Merkle DB from {:?} to {version:?}",
                self.mdb_version
            );
            merkledb_shard_plumb::write_mdb_version_guard_note(
                &self.repo_dir,
                get_merkledb_notes_name,
                version,
            )?;
        }

        // Also adds a note with empty data, this ensures the particular ref notes
        // exists so git doesn't report error on push. Git blob store ensures that
        // only one copy is stored.
        match version {
            ShardVersion::V1 => {
                merkledb_plumb::add_empty_note(&self.xet_config, get_merkledb_notes_name(version))
                    .await?
            }
            ShardVersion::V2 | ShardVersion::Uninitialized => merkledb_shard_plumb::add_empty_note(
                &self.xet_config,
                get_merkledb_notes_name(version),
            )?,
        }

        Ok(())
    }

    // Add a secure random number as salt to refs notes.
    // Do nothing if a salt already exists.
    fn set_repo_salt(&self) -> Result<bool> {
        info!("Setting repo salt.");

        let notesref = GIT_NOTES_REPO_SALT_REF_NAME;

        if self.repo.find_reference(notesref).is_ok() {
            info!("Skipping setting repo salt; {notesref} already present.");
            return Ok(false);
        }

        let notes_handle = GitNotesWrapper::from_repo(self.repo.clone(), notesref);

        let rng = ring::rand::SystemRandom::new();
        let salt: [u8; REPO_SALT_LEN] = ring::rand::generate(&rng)
            .map_err(|_| GitXetRepoError::Other("failed generating a salt".to_owned()))?
            .expose();

        notes_handle.add_note(salt).map_err(|e| {
            error!("Error inserting new note in set_repo_salt: {e:?}");
            e
        })?;

        Ok(true)
    }
}

pub mod test_tools {
    use std::fs::create_dir_all;

    use same_file::is_same_file;
    use tempfile::TempDir;
    use xet_config::Cfg;

    use super::*;

    pub struct TestRepoPath {
        pub path: PathBuf,
        _tempdir: TempDir,
    }

    impl TestRepoPath {
        pub fn new<T: AsRef<Path>>(name: T) -> Result<Self> {
            let tmp_repo = TempDir::new()?;
            let path = tmp_repo.path().join(name);
            create_dir_all(&path)?;
            Ok(Self {
                path,
                _tempdir: tmp_repo,
            })
        }

        fn from_args(path: PathBuf, tempdir: TempDir) -> Self {
            Self {
                path,
                _tempdir: tempdir,
            }
        }
    }

    pub struct TestRepo {
        pub repo: GitRepo,
        _repo_path: TestRepoPath,
    }

    impl TestRepo {
        pub fn new() -> Result<TestRepo> {
            let repo_path = TestRepoPath::new("repo")?;

            git_wrap::run_git_captured(Some(&repo_path.path), "init", &[], true, None)?;

            let git_repo = GitRepo::open(XetConfig::new(
                Some(Cfg::with_default_values()),
                None,
                ConfigGitPathOption::PathDiscover(repo_path.path.clone()),
            )?)?;

            Ok(Self {
                repo: git_repo,
                _repo_path: repo_path,
            })
        }

        #[allow(clippy::should_implement_trait)] //TODO: choose a better name
        pub fn clone(origin: &TestRepo) -> Result<TestRepo> {
            let tmp_repo = TempDir::new()?;
            let base_path = tmp_repo.path().to_path_buf();
            let path = base_path.join("repo");

            git_wrap::run_git_captured(
                Some(&base_path),
                "clone",
                &[origin.repo.repo_dir.to_str().unwrap(), "repo"],
                true,
                None,
            )?;

            let git_repo = GitRepo::open(XetConfig::new(
                Some(Cfg::with_default_values()),
                None,
                ConfigGitPathOption::PathDiscover(path.clone()),
            )?)?;

            Ok(Self {
                repo: git_repo,
                _repo_path: TestRepoPath::from_args(path, tmp_repo),
            })
        }

        /// Runs a bunch of tests to ensure that the current
        /// repo is configured correctly.
        pub fn test_consistent(&self) -> Result<()> {
            let repo_dir_1 = self.repo.git_dir.clone();
            let repo_dir_query = self.repo.repo_dir.join(
                self.repo
                    .run_git_checked_in_repo("rev-parse", &["--git-dir"])?,
            );

            assert!(is_same_file(repo_dir_1, repo_dir_query)?);

            let repo_dir_1 = self.repo.repo_dir.clone();
            let repo_dir_query = self.repo.repo_dir.join(
                self.repo
                    .run_git_checked_in_repo("rev-parse", &["--show-toplevel"])?,
            );

            assert!(is_same_file(repo_dir_1, repo_dir_query)?);

            Ok(())
        }

        pub fn write_file(&self, filename: &str, seed: u64, size: usize) -> Result<()> {
            use rand::prelude::*;
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut data = vec![0u8; size];
            rng.fill_bytes(&mut data[..]);
            let path = self.repo.repo_dir.join(filename);
            create_dir_all(path.parent().unwrap())?;
            File::create(&path)?.write_all(&data)?;

            Ok(())
        }
    }
}

#[cfg(test)]
mod git_repo_tests {
    use std::fs::OpenOptions;

    use super::test_tools::TestRepo;
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_repo_query_functions() -> Result<()> {
        let tr = TestRepo::new()?;

        tr.test_consistent()?;

        // Test to make sure the cleanliness of the repository is correctly reported.
        assert!(tr.repo.repo_is_clean()?);

        tr.write_file("test_file.dat", 0, 100)?;
        tr.repo.run_git_checked_in_repo("add", &["test_file.dat"])?;

        assert!(!tr.repo.repo_is_clean()?);

        tr.repo
            .run_git_checked_in_repo("commit", &["-m", "Added test_file.dat"])?;

        assert!(tr.repo.repo_is_clean()?);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_repo_remotes() -> Result<()> {
        let tr1 = TestRepo::new()?;
        let tr2 = TestRepo::new()?;
        let tr3 = TestRepo::new()?;

        tr1.repo.run_git_checked_in_repo(
            "remote",
            &["add", "tr2", tr2.repo.repo_dir.to_str().unwrap()],
        )?;
        tr1.repo.run_git_checked_in_repo(
            "remote",
            &["add", "tr3", tr3.repo.repo_dir.to_str().unwrap()],
        )?;

        let mut remotes = tr1.repo.current_remotes()?;
        remotes.sort();

        assert_eq!(remotes, &["tr2", "tr3"]);

        let mut remotes_2 = GitRepo::list_remotes(&tr1.repo.repo)?;
        remotes_2.sort();

        assert_eq!(remotes_2[0].0, "tr2");
        assert_eq!(remotes_2[1].0, "tr3");

        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fetch_config_added() -> Result<()> {
        let tr_origin = TestRepo::new()?;
        let mut tr = TestRepo::clone(&tr_origin)?;

        let query_config = |tr: &TestRepo| {
            let (_, config_settings, _) = tr
                .repo
                .run_git_in_repo(
                    "config",
                    &["--get-regex", "remote\\.[a-z]+\\.fetch", ".*/xet/.*"],
                )
                .unwrap();
            config_settings.trim().to_owned()
        };

        let current_notes_config = query_config(&tr);
        assert_eq!(current_notes_config, "");

        tr.repo.initialize(true, false).await?;

        let current_notes_config = query_config(&tr);
        assert!(current_notes_config.contains("origin"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_write_hook() -> Result<()> {
        let tr = TestRepo::new()?;

        let subpath = "foo";
        let hook_file = tr.repo.git_dir.join(subpath);

        // test writing an empty file
        let res = tr.repo.write_hook(subpath, "bar")?;
        assert!(res);

        let contents =
            fs::read_to_string(hook_file.clone()).expect("Hook file wasn't written correctly");

        assert_eq!(contents, "#!/usr/bin/env bash\nbar");

        // append some other hooks
        let mut file = OpenOptions::new()
            .append(true)
            .open(hook_file.clone())
            .unwrap();
        writeln!(file, "\nbaz")?;

        let contents =
            fs::read_to_string(hook_file.clone()).expect("Hook file wasn't written correctly");
        assert_eq!(contents, "#!/usr/bin/env bash\nbar\nbaz\n");

        // ensure write_hooks keeps it the same and doesn't write when script is in hooks
        let res = tr.repo.write_hook(subpath, "bar")?;
        assert!(!res);

        let contents =
            fs::read_to_string(hook_file.clone()).expect("Hook file wasn't written correctly");

        assert_eq!(contents, "#!/usr/bin/env bash\nbar\nbaz\n");

        // ensure write_hooks appends our script when it doesn't exist in previous file
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(hook_file.clone())
            .unwrap();
        writeln!(file, "#!/usr/bin/env bash\nbaz")?;

        let contents =
            fs::read_to_string(hook_file.clone()).expect("Hook file wasn't written correctly");
        assert_eq!(contents, "#!/usr/bin/env bash\nbaz\n");

        let res = tr.repo.write_hook(subpath, "bar")?;
        assert!(res);

        let contents = fs::read_to_string(hook_file).expect("Hook file wasn't written correctly");

        assert_eq!(contents, "#!/usr/bin/env bash\nbaz\nbar\n");

        Ok(())
    }

    #[test]
    fn test_gitattributes_regex_match() {
        // Test that all valid versions of the .gitattribute match the regex
        assert!(GITATTRIBUTES_TEST_REGEX.is_match("* filter=xet diff=xet merge=xet -text"));
        assert!(GITATTRIBUTES_TEST_REGEX.is_match("* filter=xet -text"));
        assert!(GITATTRIBUTES_TEST_REGEX.is_match("*.gitattributes filter="));
        assert!(GITATTRIBUTES_TEST_REGEX.is_match("*.xet/** filter="));
    }
}
