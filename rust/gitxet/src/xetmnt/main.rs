use clap::Parser;
use gitxetcore::command::CliOverrides;
use gitxetcore::config::ConfigGitPathOption;
use gitxetcore::config::XetConfig;
use gitxetcore::environment::log::initialize_tracing_subscriber;
use gitxetcore::xetmnt::perform_mount_and_wait_for_ctrlc;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[clap(version = "0.1.0", propagate_version = true)]
#[clap(about = "Xetmnt command line", long_about = None)]
pub struct XetmntCommand {
    #[clap(flatten)]
    pub overrides: CliOverrides,
    pub xet: PathBuf,
    pub mount: PathBuf,
    #[clap(short, long, default_value = "HEAD")]
    pub reference: String,

    /// how many 32MB blocks to prefetch after a read
    #[clap(short, long, default_value = "16")]
    pub prefetch: usize,

    /// Experimental writable
    #[clap(short, long)]
    pub writable: bool,
}

#[tokio::main]
async fn main() {
    let cli = XetmntCommand::parse();
    std::env::set_current_dir(&cli.xet).unwrap();
    // assert that .git exists and that this is a valid repo
    if !cli.xet.join(".git").exists() {
        eprintln!("Error: xet path is not a git checkout");
        return;
    }
    let cfg = XetConfig::new(
        None,
        Some(cli.overrides),
        ConfigGitPathOption::CurdirDiscover,
    )
    .unwrap();
    initialize_tracing_subscriber(&cfg).unwrap();
    if let Err(e) = perform_mount_and_wait_for_ctrlc(
        cfg,
        &cli.xet,
        &cli.mount,
        &cli.reference,
        false,
        cli.prefetch,
        cli.writable,
        "127.0.0.1:0".to_owned(),
        || {},
        None,
    )
    .await
    {
        eprintln!("Error: {e:?}");
    }
}
