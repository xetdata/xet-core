use std::path::PathBuf;
use std::str::FromStr;

use clap::{Args, Subcommand};
use tracing::{error, warn};

use crate::config::XetConfig;
use crate::errors::Result;
use crate::git_integration::repo_migration::migrate_repo;
use crate::git_integration::{clone_xet_repo, run_git_captured, run_git_passthrough, GitXetRepo};
use path_absolutize::*;

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

#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum RepoSubCommand {
    /// Migrate an external repository to a new XetHub repository. All commits, branches,
    /// and other files are converted, history is fully preserved, and all data files stored
    /// as LFS or Xet pointer files are imported.
    Migrate(MigrateArgs),
}

#[derive(Args, Debug)]
pub struct RepoCommandShim {
    #[clap(subcommand)]
    subcommand: RepoSubCommand,
}

impl RepoCommandShim {
    pub fn subcommand_name(&self) -> String {
        match self.subcommand {
            RepoSubCommand::Migrate(_) => "migrate".to_string(),
        }
    }
}

pub async fn repo_command(config: XetConfig, args: &RepoCommandShim) -> Result<()> {
    match &args.subcommand {
        RepoSubCommand::Migrate(migrate_args) => migrate_command(config, migrate_args).await,
    }
}

async fn migrate_command(config: XetConfig, args: &MigrateArgs) -> Result<()> {
    let working_dir = {
        if let Some(wd) = args.working_dir.as_ref() {
            PathBuf::from_str(wd).unwrap()
        } else {
            let migration_tag = uuid::Uuid::now_v6(&[0; 6]).to_string();
            config.xet_home.join("migration").join(migration_tag)
        }
    };

    let working_dir = working_dir.absolutize()?;

    config.permission.create_dir_all(&working_dir)?;

    let source_dir = working_dir.join("source");
    let dest_dir = working_dir.join("xet_repo");

    eprintln!("XET: Retrieving Source Repo {:?}", &args.src);

    // Use --mirror here to quickly get an exact copy of the remote repo, including all the local branches.
    // Also, we don't need to push anything, so --mirror works great.
    if let Err(e) = run_git_passthrough(
        None, // Run in current directory so relative paths work.
        "clone",
        &["--mirror", &args.src, source_dir.to_str().unwrap()],
        true,
        true,
        None,
    ) {
        eprintln!("Error cloning source repository at {:?}: {e:?}", &args.src);
        eprintln!("\nPlease ensure the source repository url is correct and you have permission to access it.");
        eprintln!("\nAlternatively, you may manually clone the repository using");
        eprintln!("\n  git clone --mirror {} ", &args.src);
        eprintln!("\nthen pass the resulting local repository location to this command using --src=<local repository>.");
        Err(e)?;
        unreachable!();
    }

    eprintln!("XET: Cloning Remote Xet Repo {:?}", &args.dest);

    // Use --bare here instead of --mirror to allow us to push to all the remote branches.
    if let Err(e) = clone_xet_repo(
        Some(&config),
        &["--bare", &args.dest, dest_dir.to_str().unwrap()],
        true,
        Some(&working_dir),
        true,
        true,
        true,
    ) {
        eprintln!(
            "Error accessing destination repository at {:?}: {e:?}",
            &args.dest
        );
        eprintln!("\nPlease ensure the repository url is correct and you have run git xet login.");
        Err(e)?;
    }

    if !args.overwrite {
        // Check to make sure it's a new repository.  This is using xethub's default repo as a model, so it's not
        // the most robust.  On any error, just ask the user to use --overwrite
        let commit_count = run_git_captured(
            Some(&dest_dir),
            "rev-list",
            &["--count", "main"],
            true,
            None,
        )
        .map_err(|e| {
            warn!("Error getting commit count for testing if repo is new: {e:?}");
            e
        })
        .ok()
        .and_then(|(_, commit_count_s, _)| commit_count_s.parse::<usize>().ok())
        .unwrap_or(usize::MAX);

        if commit_count > 1 {
            return Err(crate::errors::GitXetRepoError::InvalidOperation(format!(
                "ERROR: Xet Repository {} has multiple commits on main; refusing to overwrite changes.  Use --overwrite to force operation.",
                &args.dest
            )));
        }

        let has_nonmain_branches = run_git_captured(
            Some(&dest_dir),
            "branch",
            &["-l", "--format=%(refname:short)"],
            true,
            None,
        )
        .map_err(|e| {
            warn!("Error getting branch list for repo: {e:?}");
            e
        })
        .map(|(_, branches, _)| branches.lines().any(|c| c.trim() != "main"))
        .unwrap_or(true);

        if has_nonmain_branches {
            return Err(crate::errors::GitXetRepoError::InvalidOperation(format!(
                "ERROR: Xet Repository {} has multiple branches; refusing to overwrite.  Use --overwrite to force operation.",
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
    let (branch_list, ref_list) = migrate_repo(&source_dir, &xet_repo).await?;

    eprintln!("Migration complete; packing repository at {dest_dir:?}.");
    run_git_passthrough(
        Some(&dest_dir),
        "gc",
        &["--aggressive", "--prune=now"],
        true,
        true,
        None,
    )?;

    eprintln!("Uploading data and syncing remote objects; this may take some time.");
    run_git_passthrough(
        Some(&dest_dir),
        "push",
        &["--force", "--set-upstream", "origin", "main"],
        true,
        false,
        Some(&[("XET_DISABLE_HOOKS", "0")]),
    ).map_err(|e| {
        eprintln!("Error pushing to remote.");
        eprintln!("Please go to directory {dest_dir:?} and run `git push --force --set-upstream origin main` to push manually.");
        e
    })?;

    // Push at most a subset of branches so we don't overwhelm the endpoint.
    let mut slice_size = 16;
    let remaining_branches: Vec<_> = branch_list.iter().filter(|s| *s != "main").collect();

    let mut start_idx = 0;

    while start_idx < remaining_branches.len() {
        let branches_this_push =
            &remaining_branches[start_idx..(start_idx + slice_size).min(remaining_branches.len())];

        let mut args = vec!["--set-upstream".to_owned(), "origin".to_owned()];
        args.extend(branches_this_push.iter().map(|br| format!("+{br}")));

        if let Err(e) = run_git_captured(
            Some(&dest_dir),
            "push",
            &args.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
            true,
            Some(&[("XET_DISABLE_HOOKS", "1")]),
        ) {
            if slice_size == 1 {
                eprintln!("Error updating remote branch {}.", branches_this_push[0]);
                eprintln!("Please go to directory {dest_dir:?} and run `git push --force --set-upstream origin {}` to push manually.", branches_this_push[0]);

                Err(e)?;
                unreachable!();
            } else {
                // Try fewer branches at once.
                slice_size /= 2;
                continue;
            }
        }
        // Success, now loop.
        start_idx += branches_this_push.len();
        eprintln!(
            "Synced {start_idx} / {} branches.",
            remaining_branches.len()
        );
    }

    if !ref_list.is_empty() {
        eprintln!("Syncing remaining references.");
        let mut args = vec!["origin".to_owned()];

        for r in ref_list {
            args.push(format!("+{r}:{r}"));
        }

        run_git_passthrough(
        Some(&dest_dir),
        "push",
        &args.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
        true,
        false,
        Some(&[("XET_DISABLE_HOOKS", "0")]),
    ).map_err(|e| {
        eprintln!("Error pushing to remote.");
        eprintln!("Please go to directory {dest_dir:?} and run `git push origin +refs/*:refs/*` to push manually.");
        e
    })?;
    }

    eprintln!("Syncing tags.");
    run_git_passthrough(
        Some(&dest_dir),
        "push",
        &["origin", "--force", "--tags", "--follow-tags"],
        true,
        false,
        Some(&[("XET_DISABLE_HOOKS", "0")]),
    ).map_err(|e| {
        eprintln!("Error pushing to remote.");
        eprintln!("Please go to directory {dest_dir:?} and run `git push origin --force --tags --follow-tags --all` to push manually.");
        e
    })?;

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
