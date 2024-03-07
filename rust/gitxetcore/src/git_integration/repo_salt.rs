use super::GitRepo;
use crate::constants::*;
use crate::errors::{GitXetRepoError, Result};
use std::path::Path;
use std::sync::Arc;
use tracing::info;

pub type RepoSalt = [u8; REPO_SALT_LEN];

pub fn read_repo_salt_by_dir(git_dir: &Path) -> Result<Option<RepoSalt>> {
    read_repo_salt(&GitRepo::open(Some(git_dir))?)
}

// Read one blob from the notesref as salt.
// Return error if find more than one note.
pub fn read_repo_salt(repo: &Arc<GitRepo>) -> Result<Option<RepoSalt>> {
    let notesref = GIT_NOTES_REPO_SALT_REF_NAME;

    if repo.read().find_reference(notesref).is_err() {
        info!("Repository does not appear to contain {notesref}, salt not found.");
        return Ok(None);
    }

    let mut iter = repo.xet_notes_content_iterator(notesref)?;

    let Some(Ok((_, salt_data))) = iter.next() else {
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

    let mut ret = [0u8; REPO_SALT_LEN];
    ret.copy_from_slice(&salt_data);

    Ok(Some(ret))
}
