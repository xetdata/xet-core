use std::path::PathBuf;
use std::str::FromStr;

use clap::Args;
use tracing::error;

use crate::config::XetConfig;
use crate::errors::Result;
use crate::git_integration::repo_migration::migrate_repo;
use crate::git_integration::{clone_xet_repo, run_git_captured, run_git_passthrough, GitXetRepo};

#[derive(Args, Debug, Clone)]
pub struct MigrateArgs {
    /// The URL of the git repository to import.
    #[clap(long)]
    pub src: String,

    /// The URL of the Xet repository to initialize with the contents of the src repository.  
    /// This repository must be a new Xet repo, or use --overwrite to replace all contents of
    /// this repository with the source repo.
    #[clap(long)]
    pub dest: String,

    /// The migrate command will error out if the remote repository is not empty.  
    /// When this flag is passed, the contents of the remote repository will be
    /// overwritten regardless of state.
    #[clap(long)]
    pub overwrite: bool,

    /// Do not clean up the working directories after completion.
    #[clap(long)]
    pub no_cleanup: bool,

    /// The directory to use to do all of the processing in (default: ~/.xet/migration).
    #[clap(long)]
    pub working_dir: Option<String>,
}

pub async fn migrate_command(config: XetConfig, args: &MigrateArgs) -> Result<()> {
    let working_dir = {
        if let Some(wd) = args.working_dir.as_ref() {
            PathBuf::from_str(wd).unwrap()
        } else {
            let migration_tag = uuid::Uuid::now_v6(&[0; 6]).to_string();
            config.xet_home.join("migration").join(migration_tag)
        }
    };

    std::fs::create_dir_all(&working_dir)?;

    let working_dir = working_dir.canonicalize()?;

    let source_dir = working_dir.join("source");
    let dest_dir = working_dir.join("xet_repo");

    eprintln!("XET: Retrieving Source Repo {:?}", &args.src);

    run_git_passthrough(
        Some(&working_dir),
        "clone",
        &["--mirror", &args.src, source_dir.to_str().unwrap()],
        true,
        true,
        None,
    )?;

    eprintln!("XET: Cloning Remote Xet Repo {:?}", &args.dest);

    clone_xet_repo(
        Some(&config),
        &["--bare", &args.dest, dest_dir.to_str().unwrap()],
        true,
        Some(&working_dir),
        true,
        true,
        true,
    )?;

    if !args.overwrite {
        // Check to make sure it's a new repository.
        let (_, commit_count, _) = run_git_captured(
            Some(&dest_dir),
            "rev-list",
            &["--count", "HEAD"],
            true,
            None,
        )?;

        if commit_count.parse::<usize>().unwrap_or(1) > 1 {
            return Err(crate::errors::GitXetRepoError::InvalidOperation(format!(
                "ERROR: Xet Repository {} not a new repository; refusing to overwrite changes.",
                &args.dest
            )));
        }
    }

    // Now, push everything to the remote.
    let config = config.switch_repo_path(
        crate::config::ConfigGitPathOption::PathDiscover(dest_dir.clone()),
        None,
    )?;

    // This will open and configure everything
    let xet_repo = GitXetRepo::open_and_verify_setup(config.clone()).await?;

    // Now do the actual migration process.
    migrate_repo(&source_dir, &xet_repo).await?;

    // Finally, push everything, overwriting the end.
    eprintln!("Migration complete; packing repository at {dest_dir:?}.");

    run_git_passthrough(
        Some(&dest_dir),
        "gc",
        &["--aggressive", "--prune=now"],
        true,
        true,
        None,
    )?;

    eprintln!("Uploading data and syncing remote repo.");
    run_git_passthrough(
        Some(&dest_dir),
        "push",
        &["--force"],
        true,
        true,
        Some(&[("XET_DISABLE_HOOKS", "0")]),
    )?;

    // This command may fail due to the --all (504 error) as sometimes it overwhelms the endpoint?  So
    // run regular push first, then push all the branches.  This seems to work consistently
    run_git_passthrough(
        Some(&dest_dir),
        "push",
        &["--all", "--force"],
        true,
        true,
        Some(&[("XET_DISABLE_HOOKS", "0")]),
    )?;

    if !args.no_cleanup {
        eprintln!("Cleaning up.");
        let _ = std::fs::remove_dir_all(&source_dir).map_err(|e| {
            error!("Error cleaning up directory {source_dir:?}; remove manually.");
            e
        });

        let _ = std::fs::remove_dir_all(&dest_dir).map_err(|e| {
            error!("Error cleaning up directory {dest_dir:?}; remove manually.");
            e
        });
    }

    eprintln!("\nRepository at {:?} is now set up to match {:?}.  Run \n\n  git xet clone {:?}\n\nto use the new Xet repository.", &args.dest, &args.src, &args.dest);

    Ok(())
}
