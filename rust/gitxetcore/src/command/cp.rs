use clap::Args;

use crate::config::XetConfig;
use crate::errors::Result;
use crate::xetblob::{perform_copy, XetRepoManager};

#[derive(Args, Debug)]
pub struct CpArgs {
    /// Source files or directories to copy
    #[clap(required = true, multiple_values = true, number_of_values = 1)]
    sources: Vec<String>,

    /// Destination path
    #[clap(required = true)]
    destination: String,

    /// Copy directories recursively
    #[clap(short, long)]
    recursive: bool,
}

pub async fn cp_command(cfg: XetConfig, cp_args: &CpArgs) -> Result<()> {
    let mut repo_manager = XetRepoManager::new(Some(cfg), None)?;

    perform_copy(
        &mut repo_manager,
        &cp_args.sources,
        cp_args.destination.clone(),
        cp_args.recursive,
    )
    .await
}
