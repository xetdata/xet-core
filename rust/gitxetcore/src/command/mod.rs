use clap::{Args, Parser, Subcommand};
use const_format::concatcp;
use git_version::git_version;
use opentelemetry::global::force_flush_tracer_provider;
use std::path::PathBuf;
use tracing::{debug, info, Instrument};

use cas_plumb::{handle_cas_plumb_command, CasSubCommandShim};
use checkout::{checkout_command, CheckoutArgs};
use clone::{clone_command, CloneArgs};
use config::{handle_config_command, ConfigArgs};
use cp::{cp_command, CpArgs};
use dematerialize::{dematerialize_command, DematerializeArgs};
use diff::{diff_command, DiffArgs};
use dir_summary::{dir_summary_command, DirSummaryArgs};
use filter::filter_command;
use init::{init_command, InitArgs};
use install::{install_command, InstallArgs};
use lazy::{lazy_command, LazyCommandShim};
use login::{login_command, LoginArgs};
use materialize::{materialize_command, MaterializeArgs};
use merkledb::{handle_merkledb_plumb_command, MerkleDBSubCommandShim};
use mount::{mount_command, mount_curdir_command, MountArgs, MountCurdirArgs};
use openssl_probe;
use pointer::{pointer_command, PointerArgs};
use push::push_command;
use repo_size::{repo_size_command, RepoSizeArgs};
use s3config::{s3config_command, S3configArgs};
use smudge::{smudge_command, SmudgeArgs};
use summary::{summary_command, SummaryArgs};
use uninit::{uninit_command, UninitArgs};
use uninstall::{uninstall_command, UninstallArgs};
use visualization_dependencies::{
    visualization_dependencies_command, VisualizationDependenciesArgs,
};

use crate::config::XetConfig;
use crate::config::{get_sanitized_invocation_command, ConfigGitPathOption};
use crate::constants::CURRENT_VERSION;
use crate::data::remote_shard_interface::{GlobalDedupPolicy, SmudgeQueryPolicy};
use crate::environment::axe::Axe;
use crate::environment::log::{get_trace_span, initialize_tracing_subscriber};
use crate::environment::upgrade_checks::VersionCheckInfo;
use crate::errors;
use crate::git_integration::git_version_checks::perform_git_version_check;
use crate::git_integration::hook_command_entry::{handle_hook_plumb_command, HookCommandShim};

use self::repo::{repo_command, RepoCommandShim};

mod cas_plumb;
mod checkout;
mod clone;
mod config;
mod cp;
mod dematerialize;
mod diff;
mod dir_summary;
mod filter;
pub mod init;
mod install;
mod lazy;
pub mod login;
mod materialize;
mod merkledb;
pub mod mount;
mod pointer;
mod push;
mod repo;
mod repo_size;
mod s3config;
mod smudge;
mod summary;
pub mod uninit;
mod uninstall;
mod visualization_dependencies;

#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum Command {
    /// Hydrates, Xet objects converting Xet Pointer files to real files
    #[clap(hide = true)]
    Checkout(CheckoutArgs),

    /// Run the filter process.
    #[clap(hide = true)]
    Filter,

    #[clap(hide = true)]
    Pointer(PointerArgs),

    #[clap(hide = true)]
    Smudge(SmudgeArgs),

    /// Manually push all staged cas information to a remote CAS.
    #[clap(hide = true)]
    Push,

    /// Plumbing commands for merkledb integration.
    Merkledb(MerkleDBSubCommandShim),

    /// Plumbing commands for cas.
    Cas(CasSubCommandShim),

    /// Plumbing commands for the git integration hooks.
    #[clap(hide = true)]
    Hooks(HookCommandShim),

    /// Clones an existing git xet repo into a new directory.
    Clone(CloneArgs),

    /// Installs the git filter config.
    Install(InstallArgs),

    /// Configures the local repository to use git xet.
    #[clap(hide = true)]
    Init(InitArgs),

    /// Manage the xet config.  
    Config(ConfigArgs),

    /// Computes and returns the total repo size (for the current commit),
    /// cached in git notes.
    RepoSize(RepoSizeArgs),

    /// Computes and returns a file-level summary for a given file in the repo.
    /// Stores the result in git notes.
    #[clap(hide = true)]
    Summary(SummaryArgs),

    /// Computes and returns a directory-level summary for all directories in the repo.
    #[clap(hide = true)]
    DirSummary(DirSummaryArgs),

    /// Computes a summary-diff for a provided file between two commits.
    #[clap(hide = true)]
    Diff(DiffArgs),

    /// Mounts a repository on a local path
    Mount(MountArgs),

    /// Uninstall git config information.
    Uninstall(UninstallArgs),

    /// Uninstall git xet hooks and components from the local repository.
    #[clap(hide = true)]
    Uninit(UninitArgs),

    #[clap(hide = true)]
    MountCurdir(MountCurdirArgs),

    /// Computes and returns the data dependencies of custom visualizations,
    /// cached in git notes.
    #[clap(hide = true)]
    VisualizationDependencies(VisualizationDependenciesArgs),

    /// Stores authentication information for Xethub
    Login(LoginArgs),

    /// Auxiliary commands to manage the lazy config.
    Lazy(LazyCommandShim),

    /// Materialize files and add the list of file paths to the lazy config.
    Materialize(MaterializeArgs),

    /// Dematerialize files and drop the list of file paths from the lazy config.
    Dematerialize(DematerializeArgs),

    /// Copy files to/from a xet remote.  
    #[clap(hide = true)]
    Cp(CpArgs),

    /// Configure access to the XetHub S3 service.
    S3config(S3configArgs),

    /// Repo level actions such as migrating repos.
    Repo(RepoCommandShim),
}

const GIT_VERSION: &str = git_version!(
    args = ["--always", "--dirty", "--exclude=*"],
    fallback = "unknown"
);
const VERSION: &str = concatcp!(CURRENT_VERSION, "-", GIT_VERSION);

#[derive(Parser, Debug)]
#[clap(name="git-xet", version = CURRENT_VERSION, long_version = VERSION, propagate_version = true)]
#[clap(about = "git-xet command line", long_about = None)]
pub struct GitXetCommand {
    #[clap(flatten)]
    pub overrides: CliOverrides,

    #[clap(subcommand)]
    pub command: Command,
}

/// Overrides config settings with ones supplied on the CLI
#[derive(Args, Debug, Default, Clone)]
pub struct CliOverrides {
    /// Increases verbosity of output. Multiple -v's can be added to increase verbosity.
    #[clap(long, short = 'v', parse(from_occurrences))]
    pub verbose: i8,

    #[clap(long, hide = true)]
    pub disable_version_check: bool,

    /// Sets the output log file. Writes to stderr if not provided.
    #[clap(long, short)]
    pub log: Option<PathBuf>,

    /// Optionally override cas endpoint.
    #[clap(long, short)]
    pub cas: Option<String>,

    /// Sets the shard reconstruction policy for the
    #[clap(long, hide = true)]
    pub smudge_query_policy: Option<SmudgeQueryPolicy>,

    /// Sets the global dedup policy for when to query the shard server for other shards to dedup against
    #[clap(long, hide = true)]
    pub global_dedup_query_policy: Option<GlobalDedupPolicy>,

    /// Sets the location for the merkledb file.  Defaults to <repo>/.xet/merkledb
    #[clap(long, short, hide = true)]
    pub merkledb: Option<PathBuf>,

    /// Sets the location for the merkledb-v2 cache directory. Defaults to <repo>/.xet/merkledbv2-cache
    ///
    /// This inherently also sets the location for two associate cache files, one
    /// by appending extention "meta", the other by "HEAD", which keep track of
    /// merkledb v2 refs notes.
    #[clap(long, hide = true)]
    pub merkledb_v2_cache: Option<PathBuf>,

    /// Sets the location for the merkledb-v2 session directory. Defaults to <repo>/.xet/merkledbv2-session
    #[clap(long, hide = true)]
    pub merkledb_v2_session: Option<PathBuf>,

    /// Specify a specific config profile to use. By default, a profile will be chosen based
    /// off of the remote endpoint in the repo (if one exists), using the default settings for
    /// xethub.com if no profiles are found.
    ///
    /// Be careful with using this as it is a high priority override of the config.
    #[clap(long, short, hide = true)]
    pub profile: Option<String>,

    /// Overrides the user name used to authentication.
    /// Be careful with using this as it is a high priority override of the config.
    #[clap(long, hide = true)]
    pub user_name: Option<String>,

    /// Overrides the user token used to authentication.
    /// Be careful with using this as it is a high priority override of the config.
    #[clap(long, hide = true)]
    pub user_token: Option<String>,

    /// Overrides the user email used to authentication.
    /// Be careful with using this as it is a high priority override of the config.
    #[clap(long, hide = true)]
    pub user_email: Option<String>,

    /// Overrides the user login_id used to authentication.
    /// Be careful with using this as it is a high priority override of the config.
    #[clap(long, hide = true)]
    pub user_login_id: Option<String>,
}

impl Command {
    pub async fn run(&self, cfg: XetConfig) -> errors::Result<()> {
        let axe_cfg = cfg.clone();
        let axe = Axe::command_start(&self.name(), &axe_cfg).await;
        let ret = match self {
            Command::Checkout(args) => checkout_command(&cfg, args).await,
            Command::Filter => filter_command(cfg).await,
            Command::Pointer(args) => pointer_command(args),
            Command::Smudge(args) => smudge_command(&cfg, args).await,
            Command::Push => push_command(cfg).await,
            Command::Merkledb(args) => handle_merkledb_plumb_command(cfg, args).await,
            Command::Cas(args) => handle_cas_plumb_command(&cfg, args).await,
            Command::Hooks(args) => handle_hook_plumb_command(cfg, args).await,
            Command::Clone(args) => clone_command(cfg, args).await,
            Command::Install(args) => install_command(cfg, args).await,
            Command::Init(args) => init_command(cfg, args).await,
            Command::Config(args) => handle_config_command(args),
            Command::RepoSize(args) => repo_size_command(cfg, args).await,
            Command::Summary(args) => summary_command(cfg, args).await,
            Command::DirSummary(args) => dir_summary_command(cfg, args).await,
            Command::Diff(args) => diff_command(cfg, args).await,
            Command::Mount(args) => mount_command(&cfg, args).await,
            Command::MountCurdir(args) => mount_curdir_command(cfg, args).await,
            Command::Uninstall(args) => uninstall_command(cfg, args).await,
            Command::Uninit(args) => uninit_command(cfg, args).await,
            Command::Login(args) => login_command(cfg, args).await,
            Command::VisualizationDependencies(args) => {
                visualization_dependencies_command(cfg, args).await
            }
            Command::Lazy(args) => lazy_command(cfg, args).await,
            Command::Materialize(args) => materialize_command(cfg, args).await,
            Command::Dematerialize(args) => dematerialize_command(cfg, args).await,
            Command::Cp(args) => cp_command(cfg, args).await,
            Command::S3config(args) => s3config_command(cfg, args),
            Command::Repo(args) => repo_command(cfg, args).await,
        };
        if let Ok(mut axe) = axe {
            axe.command_complete().await;
        }
        ret
    }

    pub fn allow_version_check(&self) -> bool {
        match self {
            Command::Checkout(_) => true,
            Command::Filter => true,
            Command::Pointer(_) => false,
            Command::Smudge(_) => false,
            Command::Push => true,
            Command::Merkledb(_) => false,
            Command::Cas(_) => false,
            Command::Hooks(_) => false,
            Command::Install(_) => true,
            Command::Init(_) => true,
            Command::Clone(_) => true,
            Command::Config(_) => true,
            Command::RepoSize(_) => false,
            Command::Summary(_) => false,
            Command::DirSummary(_) => false,
            Command::Diff(_) => false,
            Command::Mount(_) => true,
            Command::MountCurdir(_) => true,
            Command::Uninstall(_) => false,
            Command::Uninit(_) => false,
            Command::Login(_) => true,
            Command::Lazy(_) => false,
            Command::VisualizationDependencies(_) => false,
            Command::Materialize(_) => true,
            Command::Dematerialize(_) => true,
            Command::Cp(_) => true,
            Command::S3config(_) => true,
            Command::Repo(_) => true,
        }
    }

    pub fn name(&self) -> String {
        match self {
            Command::Checkout(_) => "checkout".to_string(),
            Command::Filter => "filter".to_string(),
            Command::Pointer(_) => "pointer".to_string(),
            Command::Smudge(_) => "smudge".to_string(),
            Command::Push => "push".to_string(),
            Command::Merkledb(args) => format!("merkledb.{}", args.subcommand_name()),
            Command::Cas(args) => format!("cas.{}", args.subcommand_name()),
            Command::Hooks(args) => format!("hooks.{}", args.subcommand_name()),
            Command::Install(_) => "install".to_string(),
            Command::Init(_) => "init".to_string(),
            Command::Clone(_) => "clone".to_string(),
            Command::Config(_) => "config".to_string(),
            Command::RepoSize(_) => "repo_size".to_string(),
            Command::Summary(_) => "summary".to_string(),
            Command::DirSummary(_) => "dir-summary".to_string(),
            Command::Diff(_) => "diff".to_string(),
            Command::Mount(_) => "mount".to_string(),
            Command::MountCurdir(_) => "mount-curdir".to_string(),
            Command::Uninstall(_) => "uninstall".to_string(),
            Command::Uninit(_) => "uninit".to_string(),
            Command::Login(_) => "login".to_string(),
            Command::VisualizationDependencies(_) => "visualization-dependencies".to_string(),
            Command::Lazy(_) => "lazy".to_string(),
            Command::Materialize(_) => "materialize".to_string(),
            Command::Dematerialize(_) => "dematerialize".to_string(),
            Command::Cp(_) => "cp".to_string(),
            Command::S3config(_) => "s3config".to_string(),
            Command::Repo(args) => format!("repo.{}", args.subcommand_name()),
        }
    }
    pub fn long_running(&self) -> bool {
        matches!(self, Command::Filter)
    }
}

/// A struct to handle the lifecycle of the git-xet app. Consisting of behavior on startup,
/// running the application, and shutdown of the app.
pub struct XetApp {
    command: Command,
    config: XetConfig,
}

impl XetApp {
    /// Initialize the app, returning a [XetApp] handle to run the application.
    ///
    /// Currently, this consists of:
    /// * parsing the CLI
    /// * extracting the config from the environment and CLI args,
    /// * starting up logging/tracing
    pub fn init() -> errors::Result<XetApp> {
        // Make sure the version of git we're using is in fact correct.
        perform_git_version_check()?;

        let mut cli = GitXetCommand::parse();

        // Disable the version check here if we need to.
        if !cli.command.allow_version_check() {
            cli.overrides.disable_version_check = true;
        }
        let cli = cli;

        // We don't validate the configuration for the `config` command
        // since, if the config is invalid, we want to allow fixing it.
        let cfg = match &cli.command {
            Command::Config(_) => XetConfig::empty(),
            _ => XetConfig::new(
                None,
                Some(cli.overrides),
                ConfigGitPathOption::CurdirDiscover,
            )?,
        };
        initialize_tracing_subscriber(&cfg)?;

        // Initialize the local ssl cert locations if appropriate
        let _ = openssl_probe::try_init_ssl_cert_env_vars();

        // Log the command used to invoke this process.
        info!(
            "Xet invoked with {}",
            get_sanitized_invocation_command(false)
        );

        Ok(XetApp {
            command: cli.command,
            config: cfg,
        })
    }

    /// Runs the command indicated by the application.
    pub async fn run(&self) -> errors::Result<()> {
        info!("Is Debug build: {:?}", is_debug_build());
        debug!("Config: {:?}", self.config);
        info!(
            "Running in directory {:?}",
            std::env::current_dir().unwrap_or_default()
        );

        let mut version_check_handle = None;

        if !self.config.disable_version_check {
            version_check_handle = Some(tokio::spawn(VersionCheckInfo::load_or_query(
                self.config.clone(),
            )));
        }

        let span = get_trace_span(&self.command);
        let ret = if self.command.long_running() {
            self.command.run(self.config.clone()).await
        } else {
            self.command.run(self.config.clone()).instrument(span).await
        };

        if let Some(jh) = version_check_handle {
            if let Ok(Some(mut vci)) = jh.await.map_err(|e| {
                info!("Error occurred on joining of version check: {e:?}.");
                e
            }) {
                vci.notify_user_if_appropriate();
            }
        }

        ret
    }
}

impl Drop for XetApp {
    /// Run operations to close the app.
    ///
    /// Currently, this consists of:
    /// * shutting down logging/tracing (which will flush pending traces to a remote service if enabled)
    fn drop(&mut self) {
        info!("{} request completed", self.command.name());
        force_flush_tracer_provider();
    }
}

fn is_debug_build() -> bool {
    #[cfg(debug_assertions)]
    return true;
    #[cfg(not(debug_assertions))]
    return false;
}
