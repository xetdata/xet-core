use clap::Args;
use std::path::PathBuf;

use crate::config::XetConfig;

#[derive(Args, Debug)]
pub struct DeMaterializeArgs {
    #[clap(short, long)]
    recursive: bool,

    path: PathBuf,
}

pub async fn dematerialize_command(config: XetConfig, args: &DeMaterializeArgs) -> Result<()> {
    
}
