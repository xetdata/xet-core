use crate::config::XetConfig;
use crate::errors;
use crate::git_integration::git_url::parse_remote_url;
use crate::git_integration::*;
use crate::xetmnt::{check_for_mount_program, perform_mount_and_wait_for_ctrlc};
use clap::Args;
use mdb_shard::shard_version::ShardVersion;
use std::fmt::Debug;
use std::path::PathBuf;
use std::{thread, time};
use tempfile::TempDir;
use tokio::process::Command;
use tracing::info;

#[cfg(windows)]
use std::str::FromStr;

#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};

/// --------
/// | NOTE |
/// --------
/// Note that any changes here will need to reflected in pyxet mount
/// as pyxet is not able to read the clap default values
/// and also has handle the windows conditional compilation.

#[derive(Args, Debug)]
pub struct MountArgs {
    /// A remote URL to a git repository. https://xethub.com/[user]/[repo]/ or xet@xethub.com:[user]/[repo]/
    pub remote: String,

    #[cfg(not(target_os = "windows"))]
    /// A local path to mount on to
    pub path: Option<PathBuf>,

    #[cfg(target_os = "windows")]
    #[clap(default_value = "X")]
    /// An unused drive letter to use for mounting the repository
    pub drive: String,

    /// An optional commit id, or branch name. If not specified, the main or master branch is used.
    #[clap(short, long, default_value = "HEAD")]
    pub reference: String,

    /// If set this runs in foreground, instead of running as a daemon
    #[clap(short, long)]
    pub foreground: bool,

    /// Prefetch aggressiveness. the number of 32MB blocks to prefetch after a read. Set to 0
    /// if mostly random access patterns are expected, and this will also affect other internal
    /// caching parameters.
    #[clap(short, long, default_value = "16")]
    pub prefetch: usize,

    #[cfg(target_os = "windows")]
    #[clap(long, default_value = "auto")]
    /// The IP address used for hosting the local nfs server mapping the remote repository.  
    /// If "localhost", then an unused local IP address will be used for mounting.
    pub ip: String,

    #[cfg(not(target_os = "windows"))]
    #[clap(long, default_value = "127.0.0.1")]
    /// The IP address used for hosting the local nfs server mapping the remote repository.  
    /// If "localhost", then an unused local IP address will be used for mounting.
    pub ip: String,

    /// The local path to clone the temporary repo directory into
    pub clonepath: Option<PathBuf>,

    /// VERY Experimental writable mount feature.
    #[clap(short, long, hide = true)]
    pub writable: bool,

    /// EXPERIMENTAL
    /// Enable watching for remote updates to the repo, periodically updating the repo with
    /// the latest commit in the remote. Requires that reference is set to a branch (not HEAD)
    /// and is incompatible with writable flag.
    #[clap(hide(true), long)]
    pub watch: Option<humantime::Duration>,

    /// Do not use. Used only the python xet mount feature
    /// required so that the when we re-exec with mount-curdir
    /// we know which python exe to pick up
    #[clap(short, long, hide = true)]
    pub invoked_from_python: Option<String>,
}

#[derive(Args, Debug)]
pub struct MountCurdirArgs {
    /// A local path to mount on to
    pub path: PathBuf,

    #[clap(short, long, default_value = "HEAD")]
    /// An optional commit id, or branch name. If not specified, the main or master branch is used.
    pub reference: String,

    #[clap(hide(true), short, long)]
    /// Sends SIGUSR1 to this pid
    pub signal: Option<i32>,

    #[clap(long)]
    /// Automatically terminates on unmount
    pub autostop: bool,

    /// Prefetch aggressiveness. the number of 32MB blocks to prefetch after a read. Set to 0
    /// if mostly random access patterns are expected, and this will also affect other internal
    /// caching parameters.
    #[clap(short, long, default_value = "16")]
    pub prefetch: usize,

    #[cfg(target_os = "windows")]
    #[clap(long, default_value = "127.0.0.1")]
    /// The IP address used for hosting the local nfs server mapping the remote repository.  
    /// If "localhost", then an unused local IP address will be used for mounting.
    pub ip: String,

    #[cfg(not(target_os = "windows"))]
    #[clap(long, default_value = "127.0.0.1")]
    /// The IP address used for hosting the local nfs server mapping the remote repository.  
    /// If "localhost", then an unused local IP address will be used for mounting.
    pub ip: String,

    /// VERY Experimental writable mount feature.
    #[clap(short, long)]
    pub writable: bool,

    /// EXPERIMENTAL
    /// Enable watching for remote updates to the repo, periodically updating the repo with
    /// the latest commit in the remote. Requires that reference is set to a branch (not HEAD)
    /// and is incompatible with writable flag.
    #[clap(hide(true), long)]
    pub watch: Option<humantime::Duration>,
}

#[cfg(not(target_os = "windows"))]
fn is_windows_home_edition() -> errors::Result<bool> {
    Ok(false)
}

#[cfg(target_os = "windows")]
fn is_windows_home_edition() -> errors::Result<bool> {
    use crate::errors::GitXetRepoError;

    let output = std::process::Command::new("wmic")
        .args(["os", "get", "caption"])
        .output()?;
    if !output.status.success() {
        return Err(GitXetRepoError::WindowsEditionCheckError);
    }
    let stdout =
        String::from_utf8(output.stdout).map_err(|_| GitXetRepoError::WindowsEditionCheckError)?;
    Ok(stdout.contains("Home")) // expect a string like Microsoft Windows 11 Home
}

#[allow(unused_variables)]
pub async fn mount_command(cfg: &XetConfig, args: &MountArgs) -> errors::Result<()> {
    GitXetRepo::write_global_xet_config()?;

    let start_time = std::time::SystemTime::now();

    if cfg!(windows) && is_windows_home_edition().unwrap_or(false) {
        return Err(errors::GitXetRepoError::Other(
            "Mount is not supported on Windows Home edition.".into(),
        ));
    }

    if !check_for_mount_program() {
        return Err(errors::GitXetRepoError::Other(
            "Unable to locate suitable mount command".into(),
        ));
    }

    let path = {
        #[cfg(target_os = "windows")]
        {
            let mut path = args.drive.clone();
            path = path.strip_suffix("\\").unwrap_or(&path).to_owned();
            path = path.strip_suffix(":").unwrap_or(&path).to_owned();

            if path.len() != 1 || !path.chars().all(char::is_alphabetic) {
                return Err(errors::GitXetRepoError::Other(
                    "Error: mount path must be an unused drive letter.".into(),
                ));
            }
            path = path.to_uppercase();
            path += ":/";
            PathBuf::from_str(&path).unwrap()
        }

        #[cfg(not(target_os = "windows"))]
        {
            let mut path = if let Some(ref path) = args.path {
                path.clone()
            } else if let Ok((_, repo, _)) = parse_remote_url(&args.remote) {
                repo.into()
            } else {
                return Err(errors::GitXetRepoError::Other("Unable to derive repository name from remote. Please explicitly specify the target mount path".into()));
            };

            if !path.has_root() {
                path = std::env::current_dir()?.join(path);
            }
            path
        }
    };

    // create a temporary clonepath is one is not provided
    let mut clone_path = if let Some(ref clonepath) = args.clonepath {
        clonepath.clone()
    } else if args.writable {
        // for writable mounts, we clone into repo_raw/
        // first get the final component
        let mut clone_filename = path
            .file_name()
            .ok_or_else(|| {
                errors::GitXetRepoError::Other("Unable to infer target directory name".into())
            })?
            .to_os_string();
        clone_filename.push("_raw");
        // update the filename
        let mut ret = path.clone();
        ret.set_file_name(clone_filename);
        ret
    } else {
        // read only clone
        let clone_dir = TempDir::new().unwrap();
        clone_dir.into_path()
    };

    if args.writable {
        eprintln!("Writable mounts are currently an EXPERIMENTAL feature. Be warned. You might lose data!!
Known issues: Performance is poor.

Raw clone at {clone_path:?}.
Mounting at {path:?}.

You can access and make arbitrary modification in the mounted path and changes
will immediately reflect in the git state in the raw clone path. 

Similarly you can perform git operations in the raw clone path and it will immediately
reflect in the mounted path. All git operations should work as expected.

If you use a git UI, point it to the raw path.
");
    } else {
        eprintln!("Mounting to {path:?}");
    }

    // mount point should not exist, or if it exists should be empty
    if path.exists() {
        let is_empty = path.read_dir().unwrap().next().is_none();
        if !is_empty {
            return Err(errors::GitXetRepoError::Other(format!(
                "Directory {path:?} is not empty"
            )));
        }
    }

    // Clone path should not exist, or if it exists should be empty
    if clone_path.exists() {
        let is_empty = clone_path.read_dir().unwrap().next().is_none();
        if !is_empty {
            return Err(errors::GitXetRepoError::Other(format!(
                "Directory {clone_path:?} is not empty"
            )));
        }
    } else {
        std::fs::create_dir(&clone_path)?;
    }

    let branch;

    if !args.writable {
        info!("Cloning into temporary directory {clone_path:?}");
        // In the path [tempdir]
        // > git clone --mirror [remote] repo
        (_, branch) = clone_xet_repo(
            Some(cfg),
            &["--mirror", &args.remote, "repo"],
            false,             // no smudge
            Some(&clone_path), // base dir
            false,             // passthrough
            false,
            true,
        )?; // check result
        clone_path.push("repo");
    } else {
        // The mutable write uses a mirror mount
        // so we need a regular clone
        // In the path [tempdir]
        // > git clone [remote] repo
        if args.reference == "HEAD" {
            // XET_NO_SMUDGE=true git clone $remote repo
            (_, branch) = clone_xet_repo(
                Some(cfg),
                &[&args.remote, "."],
                true,              // no smudge
                Some(&clone_path), // base dir
                false,             // passthrough
                false,
                true,
            )?; // check result
        } else {
            // XET_NO_SMUDGE=true git clone -b $branch $remote repo
            (_, branch) = clone_xet_repo(
                Some(cfg),
                &["-b", &args.reference, &args.remote, "."],
                true,              // no smudge
                Some(&clone_path), // base dir
                false,             // passthrough
                false,
                true,
            )?; // check result
        }
        eprintln!("Configuring...");
        // git config --local core.worktree $repopath
        run_git_captured(
            Some(&clone_path),
            "config",
            &["--local", "core.worktree", &clone_path.to_string_lossy()],
            true,
            None,
        )?;
        // git xet config --local smudge false
        run_git_captured(
            Some(&clone_path),
            "xet",
            &["config", "--local", "smudge", "false"],
            true,
            None,
        )?;
    }

    // If invoked from Python, we use argv[1]
    // else use std::env::current_exe to find ourselves if we have it
    // otherwise run "git xet"
    let mut command = if let Some(ref pythonexe) = args.invoked_from_python {
        // we will shell exec the script
        let argv: Vec<String> = std::env::args().collect();
        if argv.len() < 2 {
            return Err(errors::GitXetRepoError::Other(
                "Unable to find python script to invoke".into(),
            ));
        }
        let mut command = Command::new(pythonexe.clone());
        command.arg(argv[1].clone());
        command
    } else if let Ok(curexe) = std::env::current_exe() {
        Command::new(curexe)
    } else {
        let mut command = Command::new(get_git_executable());
        command.arg("xet");
        command
    };

    if args.prefetch == 0 {
        // if prefetch disabled, we lower caching block size to 1MB
        command.env("XET_CACHE_BLOCKSIZE", format!("{}", 1024 * 1024));
    }

    // set the current directory and the mount-curdir subcommand
    command.current_dir(&clone_path).arg("mount-curdir");

    if args.writable {
        command.arg("--writable");
    }
    command.arg("--reference");
    if let Some(br) = branch {
        command.arg(br);
    } else {
        command.arg(&args.reference);
    }
    command.arg("--ip");
    command.arg(&args.ip);
    command.arg("--prefetch");
    command.arg(format!("{}", args.prefetch));
    // if running in background, we set the autostop flag
    if !args.foreground {
        command.arg("--autostop");
    }

    // Set the --signal flag if we are running in background AND
    // able to install a signal handler
    #[cfg(unix)]
    let sigusrstream = if !args.foreground {
        let res = signal(SignalKind::user_defined1()).ok();
        if res.is_some() {
            command
                .arg("--signal")
                .arg(format!("{}", std::process::id()));
        }
        res
    } else {
        None
    };

    // add watch if requested
    if let Some(interval) = args.watch {
        command.arg("--watch");
        let interval_string = humantime::format_duration(interval.into()).to_string();
        command.arg(interval_string);
    }

    // And finally the path to mount to
    command.arg(&path);
    info!("Exec {:?}", command);

    if args.writable {
        eprintln!(
            "\n\nIf you unmount, reboot, or encounter a crash. You can remount by running
  cd {:?}
  git xet mount-curdir --writable --prefetch 16 --autostop {:?} --ip {}\n",
            clone_path, path, &args.ip
        )
    }

    if args.foreground {
        eprintln!("Mounting...");
        command.status().await?;
    } else {
        eprintln!("Mounting as a background task...");
        let child = command.spawn()?;

        #[cfg(windows)]
        {
            thread::sleep(time::Duration::from_secs(3));
        }
        #[cfg(unix)]
        {
            if let Some(mut sigusr) = sigusrstream {
                let mut process_died = false;
                let mut process_output: Option<std::process::Output> = None;
                // we wait for either process died or we got a signal
                tokio::select! {
                    _ = sigusr.recv() => {},
                    res = child.wait_with_output() => {process_died = true;
                    process_output=res.ok()},
                };
                if process_died {
                    if let Some(output) = process_output {
                        let proc_stderr = String::from_utf8_lossy(&output.stderr);
                        let proc_stdout = String::from_utf8_lossy(&output.stdout);
                        return Err(errors::GitXetRepoError::Other(
                            format!("Mount subprocess died unexpectedly. stdout={proc_stdout}, stderr={proc_stderr}")));
                    } else {
                        return Err(errors::GitXetRepoError::Other(
                            "Mount subprocess died unexpectedly".to_string(),
                        ));
                    }
                }
                if let Ok(elapsed) = start_time.elapsed() {
                    eprintln!("Mount complete in {}s", elapsed.as_secs_f32());
                }
            } else {
                // no signals. we just sleep a little bit
                thread::sleep(time::Duration::from_secs(3));
            }
        }

        // no signals. we just sleep a little bit (non-linux, non-macOS)
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        thread::sleep(time::Duration::from_secs(3));
    }
    Ok(())
}

pub async fn mount_curdir_command(cfg: XetConfig, args: &MountCurdirArgs) -> errors::Result<()> {
    eprintln!("Setting up mount point...");
    let gitrepo = GitXetRepo::open(cfg.clone())?;
    if gitrepo.mdb_version == ShardVersion::V1 {
        gitrepo.sync_notes_to_dbs().await?;
    }
    perform_mount_and_wait_for_ctrlc(
        cfg,
        &PathBuf::from("."),
        &args.path,
        &args.reference,
        args.autostop,
        args.prefetch,
        args.writable,
        args.ip.clone(),
        || {
            if let Some(_pid) = args.signal {
                #[cfg(unix)]
                // TODO: this should be implemented on windows as well, but the mechanisms for doing it are different.
                unsafe {
                    libc::kill(_pid, libc::SIGUSR1);
                }
            }
        },
        args.watch.map(|dur| dur.into()),
    )
    .await
    .map_err(|e| errors::GitXetRepoError::Other(format!("{e:?}")))
}
