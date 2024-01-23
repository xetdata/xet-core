use std::path::PathBuf;

use super::git_process_wrapping;

pub fn is_user_identity_set(path: Option<PathBuf>) -> std::result::Result<bool, git2::Error> {
    let (_, username, _) = git_process_wrapping::run_git_captured(
        path.as_ref(),
        "config",
        &["user.name"],
        false,
        None,
    )
    .map_err(|e| {
        git2::Error::from_str(&format!("Error retrieving config setting user.name: {e:?}"))
    })?;

    let (_, email, _) = git_process_wrapping::run_git_captured(
        path.as_ref(),
        "config",
        &["user.email"],
        false,
        None,
    )
    .map_err(|e| {
        git2::Error::from_str(&format!(
            "Error retrieving config setting user.email: {e:?}"
        ))
    })?;

    Ok(!(username.trim().is_empty() || email.trim().is_empty()))
}

pub fn verify_user_config_on_clone(path: Option<PathBuf>) -> std::result::Result<(), git2::Error> {
    if !is_user_identity_set(path)? {
        return Err(git2::Error::from_str(
            "Configure your Git user name and email before running git-xet commands. \
\n\n  git config --global user.name \"<Name>\"\n  git config --global user.email \"<Email>\"",
        ));
    }

    Ok(())
}
