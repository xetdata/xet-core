use lazy::lazy_pathlist_config::check_or_create_lazy_config;
use lazy::XET_LAZY_CLONE_ENV;
use tracing::{info, info_span};
use tracing_futures::Instrument;

use crate::config::XetConfig;
use crate::constants::{self, GIT_LAZY_CHECKOUT_CONFIG};
use crate::data_processing::PointerFileTranslator;
use crate::errors;
use crate::git_integration::git_repo::GitRepo;
use crate::stream::git_stream::GitStreamInterface;

/// Filter command handler
pub async fn filter_command(config: XetConfig) -> errors::Result<()> {
    eprintln!("git-xet {} filter started", constants::CURRENT_VERSION);
    info!("Establishing Git Handshake.");

    // Sync up the notes to the local mdb
    GitRepo::verify_repo_for_filter(config.clone()).await?;

    // Setting up the lazy config is delegated from clone.
    let lazy_config_override =
        if std::env::var(XET_LAZY_CLONE_ENV).unwrap_or_else(|_| "0".to_owned()) != "0" {
            let config_file = config.repo_path()?.join(GIT_LAZY_CHECKOUT_CONFIG);
            check_or_create_lazy_config(&config_file).await?;
            Some(config_file)
        } else {
            None
        };

    let config = match lazy_config_override {
        Some(path) => XetConfig {
            lazy_config: Some(path),
            ..config.clone()
        },
        None => config,
    };

    let repo = PointerFileTranslator::from_config(&config).await?;
    let mut event_loop =
        GitStreamInterface::new_with_progress(std::io::stdin(), std::io::stdout(), repo);
    event_loop
        .establish_git_handshake()
        .instrument(info_span!("git_handshake"))
        .await?;
    info!("XET: Git Handshake established.");

    event_loop.run_git_event_loop().await?;
    event_loop
        .repo
        .write() // acquire write lock
        .await
        .finalize()
        .instrument(info_span!("finalize"))
        .await?;
    event_loop.repo.read().await.print_stats();
    Ok(())
}
