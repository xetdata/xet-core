use std::sync::Arc;

use git2::Repository;
use tracing::info;

use crate::{
    config::XetConfig,
    constants::{XET_BACKUP_COMMIT_EMAIL, XET_BACKUP_COMMIT_NAME},
};

// Returns the user name and email from available sources
pub fn get_user_info_for_commit(
    config: Option<&XetConfig>,
    repo: Arc<Repository>,
) -> (String, String) {
    let git_config = repo.config().ok();

    let user_name = git_config
        .as_ref()
        .and_then(|cfg| cfg.get_str("user.name").ok().map(|s| s.to_owned()))
        .or_else(|| config.and_then(|c| c.user.name.clone()))
        .unwrap_or_else(|| XET_BACKUP_COMMIT_NAME.to_owned());
    let user_email = git_config
        .as_ref()
        .and_then(|cfg| cfg.get_str("user.email").ok().map(|s| s.to_owned()))
        .or_else(|| config.and_then(|c| c.user.email.clone()))
        .unwrap_or_else(|| XET_BACKUP_COMMIT_EMAIL.to_owned());

    (user_name, user_email)
}

pub fn get_repo_signature(
    config: Option<&XetConfig>,
    repo: Arc<Repository>,
) -> git2::Signature<'static> {
    let (name, email) = get_user_info_for_commit(config, repo);

    // Unwrap here only
    git2::Signature::now(&name, &email)
        .unwrap_or_else(|e| {
            info!(
                "Error converting name {name} and email {email} into a signature: {e:?}; using defaults."
            );
            git2::Signature::now(XET_BACKUP_COMMIT_NAME, XET_BACKUP_COMMIT_EMAIL).unwrap()
        })
        .to_owned()
}
