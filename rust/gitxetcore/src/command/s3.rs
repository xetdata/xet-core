use clap::{Args, Subcommand};
use std::path::PathBuf;

use crate::config::XetConfig;
use crate::errors::Result;

use s3::sync_s3_url_to_local;

#[derive(Subcommand, Debug)]
pub enum S3SubCommand {
    /// Sync an s3 bucket to a given subpath.
    Import { s3_bucket: String, dest: PathBuf },
}

#[derive(Args, Debug)]
pub struct S3Args {
    #[clap(subcommand)]
    pub command: S3SubCommand,
}

pub async fn s3_command(_config: XetConfig, args: &S3Args) -> Result<()> {
    match &args.command {
        S3SubCommand::Import { s3_bucket, dest } => sync_s3_url_to_local(s3_bucket, dest).await?,
    }

    Ok(())
}
