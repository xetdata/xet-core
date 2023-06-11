use crate::config::XetConfig;
use crate::errors;
use crate::git_integration::git_repo::GitRepo;

pub async fn push_command(cfg: XetConfig) -> errors::Result<()> {
    GitRepo::open(cfg)?.upload_all_staged().await
}
