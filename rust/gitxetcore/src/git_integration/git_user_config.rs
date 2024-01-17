use std::path::PathBuf;

use super::git_process_wrapping;

pub fn get_user_config(repo_path: Option<&PathBuf>, key: &str) -> Result<String, git2::Error> {
    let (_, value, _) =
        git_process_wrapping::run_git_captured(repo_path, "config", &[key], false, None).map_err(
            |e| git2::Error::from_str(&format!("Error retrieving config setting {key}: {e:?}")),
        )?;

    Ok(value)
}

pub fn set_user_config(
    repo_path: Option<&PathBuf>,
    key: &str,
    value: &str,
) -> Result<(), git2::Error> {
    let args = if repo_path.is_none() {
        // Need to set the "global" flag if not operating inside a git repo,
        // otherwise will fail.
        vec!["--global", key, value]
    } else {
        vec![key, value]
    };

    git_process_wrapping::run_git_captured(repo_path, "config", &args, false, None).map_err(
        |e| git2::Error::from_str(&format!("Error retrieving config setting {key}: {e:?}")),
    )?;

    Ok(())
}

pub fn is_user_identity_set(path: Option<PathBuf>) -> std::result::Result<bool, git2::Error> {
    let username = get_user_config(path.as_ref(), "user.name")?;
    let email = get_user_config(path.as_ref(), "user.email")?;

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
