use crate::config::XetConfig;
use crate::errors;
use crate::git_integration::GitXetRepo;

pub async fn push_command(cfg: XetConfig) -> errors::Result<()> {
    GitXetRepo::open(cfg)?.upload_all_staged().await
}
