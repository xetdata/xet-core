use super::git_process_wrapping;
use super::git_url::{authenticate_remote_url, is_remote_url, parse_remote_url};
use super::git_xet_repo::GitXetRepo;
use crate::config::XetConfig;
use crate::errors::Result;
use std::path::PathBuf;

/// Clone a repo -- just a pass-through to git clone.
/// Return repo name and a branch field if that exists in the remote url.
pub fn clone_xet_repo(
    config: Option<&XetConfig>,
    git_args: &[&str],
    no_smudge: bool,
    base_dir: Option<&PathBuf>,
    pass_through: bool,
    allow_stdin: bool,
    check_result: bool,
) -> Result<(String, Option<String>)> {
    let mut git_args = git_args.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    // attempt to rewrite URLs with authentication information
    // if config provided

    let mut repo = String::default();
    let mut branch = None;
    if let Some(config) = config {
        for ent in &mut git_args {
            if is_remote_url(ent) {
                (*ent, repo, branch) = parse_remote_url(ent)?;
                *ent = authenticate_remote_url(ent, config)?;
            }
        }
    }
    if let Some(ref br) = branch {
        git_args.extend(vec!["--branch".to_owned(), br.clone()]);
    }

    // First, make sure that everything is properly installed and that the git-xet filter will be run correctly.
    GitXetRepo::write_global_xet_config()?;

    let smudge_arg: Option<&[_]> = if no_smudge {
        Some(&[("XET_NO_SMUDGE", "1")])
    } else {
        None
    };
    let git_args_ref: Vec<&str> = git_args.iter().map(|s| s.as_ref()).collect();

    // Now run git clone, and everything should work fine.
    if pass_through {
        git_process_wrapping::run_git_passthrough(
            base_dir,
            "clone",
            &git_args_ref,
            check_result,
            allow_stdin,
            smudge_arg,
        )?;
    } else {
        git_process_wrapping::run_git_captured(
            base_dir,
            "clone",
            &git_args_ref,
            check_result,
            smudge_arg,
        )?;
    }

    Ok((repo, branch))
}
