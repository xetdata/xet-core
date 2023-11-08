use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use anyhow::anyhow;
use clap::Args;
use git2::Repository;
use pathdiff::diff_paths;
use pbr::ProgressBar;
use tracing::{error, info, info_span, warn};
use tracing_futures::Instrument;

use pointer_file::PointerFile;

use crate::config::XetConfig;
use crate::constants::POINTER_FILE_LIMIT;
use crate::data_processing::PointerFileTranslator;
use crate::errors;
use crate::git_integration::run_git_captured;

/// Checkouts a collection of paths from the repository.
/// If no arguments provided, will checkout everything.
#[derive(Args, Debug)]
pub struct CheckoutArgs {
    /// Checkout a single file to this location
    #[clap(long)]
    pub to: Option<PathBuf>,
    /// Checkout our version of a conflicted file
    #[clap(long)]
    pub ours: bool,
    /// Checkout their version of a conflicted file
    #[clap(long)]
    pub theirs: bool,
    /// Checkout the base version of a conflicted file
    #[clap(long)]
    pub base: bool,
    pub paths: Vec<PathBuf>,
}

pub enum PathspecRelativity {
    RelativeToRepoRoot,
    RelativeToCurrentDir,
}
async fn checkout_pointer_blob<'a>(
    filename: &str,
    blob: &'a git2::Blob<'a>,
    gitxetrepo: &PointerFileTranslator,
) -> anyhow::Result<()> {
    if blob.size() > POINTER_FILE_LIMIT {
        return Err(anyhow!("Invalid pointer file"));
    }
    let pointer = PointerFile::init_from_string(std::str::from_utf8(blob.content())?, filename);
    if !pointer.is_valid() {
        return Err(anyhow!("Invalid pointer file"));
    }
    let filepath = PathBuf::from(filename);
    if let Some(parent) = filepath.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut outfile = std::fs::File::create(&filepath)?;
    if let Err(e) = gitxetrepo
        .smudge_file_from_pointer(&filepath, &pointer, &mut outfile, None)
        .instrument(info_span!("smudge_pointer_file"))
        .await
    {
        error!(
            "Failed to hydrate file {:?}: {:?}. Writing the pointer file instead.",
            filepath, e
        );
        return Err(e.into());
    }
    Ok(())
}
fn checkout_raw_blob<'a>(filename: &str, blob: &'a git2::Blob<'a>) -> anyhow::Result<()> {
    let filepath = PathBuf::from(filename);
    if let Some(parent) = filepath.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut outfile = std::fs::File::create(&filepath)?;
    outfile.write_all(blob.content())?;
    Ok(())
}

/// Checkouts the contents of the the current repository HEAD into the
/// working directory, smudging everything as needed.
///
/// repopath is any path inside a Git working directory. If not provided,
/// the current directory is used.
///
/// pathspec is used to select sub-paths to allow for checking out of
/// individual files or folders. It can comprise of filenames or globs.
/// For instance "readme.md", or "src/*".
/// If no pathspecs are provided, everything is checked out.
///
/// `pathspec_relativity` is used to inform if the pathspecs provided
/// are relative to the current working directory, or to the repository root.
///
/// Any pathspecs which cannot be resolved are skipped.
///
// TODO: move to command module + fix clippy / warnings
pub async fn checkout(
    repopath: Option<PathBuf>,
    pathspec: &[PathBuf],
    pathspec_relativity: PathspecRelativity,
    gitxetrepo: &PointerFileTranslator,
) -> errors::Result<()> {
    let repopath = match repopath {
        Some(p) => p,
        None => std::env::current_dir().map_err(|_| anyhow!("Unable to find current directory"))?,
    };
    let repo = Repository::discover(repopath)?;
    let reporoot = repo
        .workdir()
        .ok_or_else(|| anyhow!("Unable to find working directory"))?;

    let pathspec: Vec<PathBuf> = pathspec.into();

    // search for all "." and convert to "*"
    // This allows git xet checkout . to behave correctly.
    let dotpath = PathBuf::from("/");
    let pathspec: Vec<PathBuf> = pathspec
        .into_iter()
        .map(|x| if x == dotpath { PathBuf::from("*") } else { x })
        .collect();

    // resolve the PathspecRelativity
    // If it is relative to repo root, we just convert to OSString
    // Othewise, we compute a diff path before converting to OSString
    let mut pathspec: Vec<std::ffi::OsString> = match pathspec_relativity {
        PathspecRelativity::RelativeToRepoRoot => pathspec
            .iter()
            .map(|x| x.clone().into_os_string())
            .collect(),
        PathspecRelativity::RelativeToCurrentDir => {
            let curdir =
                std::env::current_dir().map_err(|_| anyhow!("Unable to find current directory"))?;
            pathspec
                .iter()
                .filter_map(|x| match diff_paths(curdir.join(x), reporoot) {
                    Some(x) => Some(x),
                    None => {
                        warn!("Unable to resolve path {:?}. Skipping.", x);
                        None
                    }
                })
                .map(|x| x.into_os_string())
                .collect()
        }
    };
    // if pathspec is empty we insert "*" to make it checkout
    // everything in the repo. Note that it is important that this happens
    // *after* we resolve pathspec relativity. (If we insert the "*" before,
    // and the relativity rules have it relative to the current directory,
    // we will only checkout that one directory).
    if pathspec.is_empty() {
        pathspec.push("*".into());
    }

    // change current directory to the repository root
    std::env::set_current_dir(reporoot)
        .map_err(|_| anyhow!("Unable to change current directory to working directory"))?;

    let gitpathspec = git2::Pathspec::new(pathspec)?;

    // find the current worktree
    let headref = repo
        .head()
        .map_err(|_| anyhow!("Unable to checkout. No head of repository"))?;
    let tree = headref
        .peel_to_tree()
        .map_err(|_| anyhow!("Unable to find tree in repository head"))?;
    // a list of filename, oid pairs to checkout
    let mut checkouts: Vec<(String, git2::Oid)> = Vec::new();

    // match the tree with the pathspec
    let matches = gitpathspec.match_tree(&tree, git2::PathspecFlags::DEFAULT)?;
    // This returns an iterator over entries
    for entry in matches.entries() {
        // test conversion to utf-8
        if let Ok(mstr) = std::str::from_utf8(entry) {
            let mpath = PathBuf::from(mstr);
            // find it in the tree
            if let Ok(ent) = tree.get_path(&mpath) {
                // find the object
                // nad insert into the checkouts list
                info!("Adding {:?}", ent.name());
                let maybe_oid = ent.to_object(&repo).map(|x| x.id());
                if let Ok(oid) = maybe_oid {
                    checkouts.push((mstr.to_string(), oid));
                }
            }
        } else {
            warn!(
                "Unable to convert path {:?} to utf8",
                String::from_utf8_lossy(entry)
            );
        }
    }
    let mut pb = ProgressBar::on(std::io::stderr(), checkouts.len() as u64);
    pb.tick(); // draw the bar immediately

    // smudge everything
    let mut updatedpaths: Vec<String> = Vec::new();
    for entry in checkouts.iter() {
        let name = &entry.0;
        let oid = entry.1;
        info!("Checking out {:?}", name);
        // if the name is < 16 characters print it all
        if name.len() < 16 {
            pb.message(&format!("{name}: "));
        } else {
            // otherwise print ... [last 12 characters of name]
            pb.message(&format!("... {}: ", &name[name.len() - 12..]));
        }
        pb.inc();
        let maybeblob = repo.find_blob(oid);
        if let Ok(blob) = maybeblob {
            if let Err(e) = checkout_pointer_blob(name, &blob, gitxetrepo)
                .await
                .or_else(|_| checkout_raw_blob(name, &blob))
            {
                warn!("Unable to checkout {}: {:?}", name, e);
            } else {
                updatedpaths.push(name.clone());
            }
        } else {
            warn!("Unable to checkout {}. Unable to read blob.", name);
        }
    }
    pb.finish();

    // push all the updated files through git update-index
    // following the process from
    // https://github.com/git-lfs/git-lfs/blob/f61dbfa56b9be91411017e3cef0d4088a3988b70/commands/pull.go
    info!("Update index {:?}", updatedpaths);
    if !updatedpaths.is_empty() {
        let cur_span = info_span!("git.update_index");
        let _handle = cur_span.enter();

        let mut updateidx = Command::new("git")
            .args(["update-index", "-q", "--refresh", "--stdin"])
            .env("GCM_INTERACTIVE", "never")
            .stdin(Stdio::piped())
            .stdout(Stdio::inherit())
            .spawn()?;

        let child_stdin = updateidx.stdin.as_mut().unwrap();
        for i in updatedpaths {
            let _ = writeln!(child_stdin, "{i}");
        }
        let _ = updateidx.wait_with_output();
    }

    // While the below simpler process might be tempting,
    // it does not update-index in the same way as above
    // I suspect this does not pass the file through the
    // clean/smudge filter and instead just adds the paths "as is"

    /*
    let mut index = repo.index()?;
    index.update_all(updatepaths, None)?;
    index.write()?;
    */

    Ok(())
}

fn get_stage(ours: bool, theirs: bool, base: bool) -> &'static str {
    // no more than 1 of ours, theirs and base is set
    assert!((ours as u8) + (theirs as u8) + (base as u8) <= 1);
    if base {
        "1:"
    } else if ours {
        "2:"
    } else if theirs {
        "3:"
    } else {
        "0:"
    }
}

/// Checkouts a single file. The filepath is relative to the current working
/// directory.
pub async fn checkout_single(
    gitxetrepo: &PointerFileTranslator,
    stage: &str,
    filepath: PathBuf,
    to: Option<PathBuf>,
) -> errors::Result<()> {
    if filepath.file_name().is_none() {
        return Err(anyhow!("We can only checkout a single file").into());
    }
    let filepath_filename = filepath.file_name().unwrap();

    let curdir =
        std::env::current_dir().map_err(|_| anyhow!("Unable to find current directory"))?;
    let repo = Repository::discover(&curdir)?;
    let reporoot = repo
        .workdir()
        .ok_or_else(|| anyhow!("Unable to find working directory"))?;

    // re-root filepath against reporoot
    let filepath = match diff_paths(curdir.join(&filepath), reporoot) {
        Some(x) => x,
        None => {
            return Err(anyhow!("Unable to resolve the path of {:?}", &filepath).into());
        }
    };
    let reference = format!(
        ":{}{}",
        stage,
        filepath
            .to_str()
            .ok_or_else(|| anyhow!("Failed to convert filepath to utf-8"))?
    );
    let (_, hash, _) = run_git_captured(None, "rev-parse", &[&reference], true, None)?;
    info!("Resolved revision to {:?}", hash);
    // we got a ref.
    // Now we need to resolve the checkout location
    let oid = git2::Oid::from_str(&hash)
        .map_err(|_| anyhow!("Unable to resolve filename in git repository"))?;

    let checkout_location = match to {
        Some(f) => {
            let f = curdir.join(f);
            if f.is_dir() {
                f.join(filepath_filename)
            } else {
                f
            }
        }
        None => filepath.clone(),
    };
    info!("Checking out to {:?}", checkout_location);
    let checkout_location = checkout_location
        .into_os_string()
        .into_string()
        .map_err(|_| anyhow!("Unable to convert path to utf-8"))?;

    let maybeblob = repo.find_blob(oid);
    if let Ok(blob) = maybeblob {
        if let Err(e) = checkout_pointer_blob(&checkout_location, &blob, gitxetrepo)
            .await
            .or_else(|_| checkout_raw_blob(&checkout_location, &blob))
        {
            warn!("Unable to checkout {:?}: {:?}", &filepath, e);
        }
    } else {
        warn!("Unable to checkout {:?}. Unable to read blob.", &filepath);
    }

    Ok(())
}

pub async fn checkout_command(cfg: &XetConfig, checkout_args: &CheckoutArgs) -> errors::Result<()> {
    let mut single_checkout: bool = false;
    if checkout_args.ours && checkout_args.theirs {
        return Err(anyhow!("Only one of --ours, --theirs or --base can be set").into());
    }
    // if to or ours, or theirs or base is set, we are checkout out 1 file
    if checkout_args.to.is_some()
        || checkout_args.ours
        || checkout_args.theirs
        || checkout_args.base
    {
        single_checkout = true;
    }
    if single_checkout && checkout_args.paths.len() != 1 {
        return Err(anyhow!(
            "We can only checkout exactly 1 file path with --to, --ours, --theirs, or --base"
        )
        .into());
    }

    if checkout_args.ours || checkout_args.theirs || checkout_args.base {
        // these are only valid during a merge conflict
        // during a merge conflict MERGE_HEAD exists
        // git rev-parse -q --verify MERGE_HEAD
        let (exitcode, _, _) = run_git_captured(
            None,
            "rev-parse",
            &["-q", "--verify", "MERGE_HEAD"],
            false,
            None,
        )?;
        match exitcode {
            Some(0) => {}
            Some(_) => {
                return Err(anyhow!(
                    "--ours, --theirs, --base flags are only valid during a merge conflict"
                )
                .into());
            }
            None => {
                eprintln!("--ours, --theirs, --base flags are only valid during a merge conflict.");
                eprintln!("But we are unable to verify if we are in one. Continuing, but we may encounter an error later");
            }
        }
    }

    let repo = PointerFileTranslator::from_config(cfg).await?;
    if single_checkout {
        checkout_single(
            &repo,
            get_stage(checkout_args.ours, checkout_args.theirs, checkout_args.base),
            checkout_args.paths[0].clone(),
            checkout_args.to.clone(),
        )
        .await?;
    } else {
        checkout(
            None,
            &checkout_args.paths[..],
            PathspecRelativity::RelativeToCurrentDir,
            &repo,
        )
        .await?;
    }
    Ok(())
}
