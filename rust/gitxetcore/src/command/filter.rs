use tracing::{info, info_span};
use tracing_futures::Instrument;

use crate::config::XetConfig;
use crate::data::PointerFileTranslator;
use crate::errors;
use crate::git_integration::GitXetRepo;
use crate::stream::git_stream::GitStreamInterface;

/// Filter command handler
pub async fn filter_command(config: XetConfig) -> errors::Result<()> {
    info!("Establishing Git Handshake.");

    // Sync up the notes to the local mdb
    GitXetRepo::open_and_verify_setup(config.clone()).await?;

    let repo = PointerFileTranslator::from_config_in_repo(&config).await?;
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
