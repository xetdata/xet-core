use crate::errors::GitXetRepoError;
use crate::errors::Result;
use itertools::Itertools;
use lazy_static::lazy_static;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Output;
use tracing::debug;

lazy_static! {
    static ref GIT_EXECUTABLE: String = {
        match std::env::var_os("XET_GIT_EXECUTABLE") {
            Some(v) => v.to_str().unwrap().to_owned(),
            None => String::default(),
        }
    };
}

pub fn get_git_executable() -> &'static str {
    if !GIT_EXECUTABLE.is_empty() {
        &(GIT_EXECUTABLE)
    } else {
        "git"
    }
}

/// Sets up the git command to run how the caller chooses.
fn spawn_git_command(
    base_directory: Option<&PathBuf>,
    command: &str,
    args: &[&str],
    env: Option<&[(&str, &str)]>,
    capture_output: bool,
    allow_stdin: bool,
) -> Result<Child> {
    let git_executable = get_git_executable();

    let mut cmd = Command::new(git_executable);
    cmd.arg(command).args(args);

    // Disable version check on recursive calls
    cmd.env("XET_DISABLE_VERSION_CHECK", "1");
    cmd.env("XET_DISABLE_HOOKS", "1");

    // Add in custom environment variables.  Note: these could override the above if needed.
    if let Some(env) = env {
        for (k, v) in env.iter() {
            cmd.env(k, v);
        }
    }

    if let Some(dir) = base_directory {
        debug_assert!(dir.exists());

        cmd.current_dir(dir);
    }
    debug!(
        "Calling git, working dir={:?}, command = git {:?} {:?}, env = {:?}",
        match base_directory {
            Some(bd) => bd.to_owned(),
            None => std::env::current_dir()?,
        },
        &command,
        &args,
        &env
    );

    // Using idea from https://stackoverflow.com/questions/30776520/closing-stdout-or-stdin

    if allow_stdin {
        cmd.stdin(std::process::Stdio::inherit());
    } else {
        // Disable stdin so it doesn't hang silently in the background.
        cmd.stdin(std::process::Stdio::piped());
    }

    // Set up the command to capture or pass through stdout and stderr
    if capture_output {
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
    } else {
        cmd.stdout(std::process::Stdio::inherit());
        cmd.stderr(std::process::Stdio::inherit());
    }

    // Spawn the child
    let mut child = cmd.spawn()?;

    // Immediately drop the writing end of the stdin pipe; if git attempts to wait on stdin, it will cause an error.
    if !allow_stdin {
        drop(child.stdin.take());
    }

    Ok(child)
}

/// Calls git directly, piping both stdout and stderr through.  
///
/// The command is run in the directory base_directory.  On nonzero exit status, an error is
/// returned.
#[tracing::instrument(skip_all, err, fields(command = command, ?args))]
pub fn run_git_captured_raw(
    base_directory: Option<&PathBuf>,
    command: &str,
    args: &[&str],
    check_result: bool,
    env: Option<&[(&str, &str)]>,
) -> Result<Output> {
    // Block use of credential manager for this bit.
    let env: Vec<(&str, &str)> = match env {
        Some(d) => {
            let mut dv = Vec::from(d);
            dv.push(("GCM_INTERACTIVE", "never"));
            dv
        }
        None => vec![("GCM_INTERACTIVE", "never")],
    };

    let child = spawn_git_command(base_directory, command, args, Some(&env), true, false)?;

    let ret = child.wait_with_output()?;

    if check_result {
        if let Some(0) = ret.status.code() {
            Ok(ret)
        } else {
            let res_stdout = std::str::from_utf8(&ret.stdout[..])
                .unwrap_or("<Binary Data>")
                .trim();
            let res_stderr = std::str::from_utf8(&ret.stderr[..])
                .unwrap_or("<Binary Data>")
                .trim();
            Err(GitXetRepoError::Other(format!(
                "Error running git command: git {:?} {:?} err_code={:?}, stdout=\"{:?}\", stderr=\"{:?}\"",
                &command, args.iter().map(|s| format!("\"{s}\"")).join(" "), &ret.status, &res_stdout, &res_stderr
            )))
        }
    } else {
        Ok(ret)
    }
}

/// Calls git directly, piping both stdout and stderr through.  
///
/// The command is run in the directory base_directory.  On nonzero exit status, an error is
/// returned.
#[tracing::instrument(skip_all, err, fields(command = command, ?args))]
pub fn run_git_captured(
    base_directory: Option<&PathBuf>,
    command: &str,
    args: &[&str],
    check_result: bool,
    env: Option<&[(&str, &str)]>,
) -> Result<(Option<i32>, String, String)> {
    let out = run_git_captured_raw(base_directory, command, args, check_result, env)?;

    let res_stdout = std::str::from_utf8(&out.stdout[..])
        .unwrap_or("<Binary Data>")
        .trim();
    let res_stderr = std::str::from_utf8(&out.stderr[..])
        .unwrap_or("<Binary Data>")
        .trim();

    debug!(
        "Git return: status = {:?}, stdout = {:?}, stderr = {:?}.",
        &out.status,
        if res_stdout.len() > 64 {
            format!("{} ... [len={:?}]", &res_stdout[..64], res_stdout.len())
        } else {
            res_stdout.to_owned()
        },
        if res_stderr.len() > 64 {
            format!("{} ... [len={:?}]", &res_stderr[..64], res_stderr.len())
        } else {
            res_stderr.to_owned()
        }
    );

    Ok((
        out.status.code(),
        res_stdout.to_owned(),
        res_stderr.to_owned(),
    ))
}

/// Calls git directly, letting stdout and stderr through to the console
/// (user will see directly see the git output).
///
/// The command is run in the directory base_directory.  On nonzero exit status, an error is
/// returned.
#[tracing::instrument(skip_all, err, fields(command = command, ?args))]
pub fn run_git_passthrough(
    base_directory: Option<&PathBuf>,
    command: &str,
    args: &[&str],
    check_result: bool,
    allow_stdin: bool,
    env: Option<&[(&str, &str)]>,
) -> Result<i32> {
    let mut child = spawn_git_command(base_directory, command, args, env, false, allow_stdin)?;

    let status = child.wait()?;

    let ret = status.code();

    match ret {
        Some(0) => Ok(0),
        Some(r) => {
            if check_result {
                Err(GitXetRepoError::Other(format!(
                    "git command returned non-zero exit code: git {:?} {:?} err_code={:?}",
                    &command,
                    args.iter().map(|s| format!("\"{s}\"")).join(" "),
                    &ret
                )))
            } else {
                Ok(r)
            }
        }
        _ => Err(GitXetRepoError::Other(format!(
            "Unknown error running git command: git {:?} {:?}",
            &command,
            args.iter().map(|s| format!("\"{s}\"")).join(" ")
        ))),
    }
}
