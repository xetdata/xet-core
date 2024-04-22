use super::git_notes_wrapper::GitNotesWrapper;
use crate::config::XetConfig;
use crate::constants::*;
use crate::errors::{GitXetRepoError, Result};
use crate::git_integration::git_repo_plumbing::open_libgit2_repo;
use git2::Repository;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

pub type RepoSalt = [u8; REPO_SALT_LEN];

pub fn generate_repo_salt() -> Result<RepoSalt> {
    let rng = ring::rand::SystemRandom::new();
    let salt: RepoSalt = ring::rand::generate(&rng)
        .map_err(|_| GitXetRepoError::Other("failed generating a salt".to_owned()))?
        .expose();

    Ok(salt)
}

pub fn repo_salt_from_bytes(bytes: &[u8]) -> Result<RepoSalt> {
    if bytes.len() != REPO_SALT_LEN {
        Err(GitXetRepoError::RepoSaltUnavailable(format!(
            "Repo salt bytes have length {}; needed {REPO_SALT_LEN}",
            bytes.len()
        )))?;
        unreachable!();
    }

    let mut repo_salt = [0u8; REPO_SALT_LEN];

    repo_salt.copy_from_slice(bytes);
    Ok(repo_salt)
}

pub fn read_repo_salt_by_dir(git_dir: &Path, config: &XetConfig) -> Result<Option<RepoSalt>> {
    let Ok(repo) = open_libgit2_repo(git_dir).map_err(|e| {
        info!("Error opening {git_dir:?} as git repository; error = {e:?}.");
        e
    }) else {
        return Ok(None);
    };

    read_repo_salt(repo, config)
}

// Read one blob from the notesref as salt.
// Return error if find more than one note.
pub fn read_repo_salt(repo: Arc<Repository>, config: &XetConfig) -> Result<Option<RepoSalt>> {
    let notesref = GIT_NOTES_REPO_SALT_REF_NAME;

    if repo.find_reference(notesref).is_err() {
        info!("Repository does not appear to contain {notesref}, salt not found.");
        return Ok(None);
    }

    let notes_wrapper = GitNotesWrapper::from_repo(repo, config, notesref)?;
    let mut iter = notes_wrapper.notes_content_iterator()?;
    let Some((_, salt_data)) = iter.next() else {
        info!("Error reading repo salt from notes: {notesref} present but empty.");
        return Ok(None);
    };

    if salt_data.len() != REPO_SALT_LEN {
        return Err(GitXetRepoError::Other(format!(
            "Repository Error: Mismatch in repo salt length from notes: {:?}",
            salt_data.len()
        )));
    }

    if iter.count() != 0 {
        return Err(GitXetRepoError::Other(
            "Repository Error: Found more than one repo salt.".to_owned(),
        ));
    }
    Ok(Some(repo_salt_from_bytes(&salt_data[..])?))
}
