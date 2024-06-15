use crate::data::{is_xet_pointer_file, PointerFileTranslatorV2};
use crate::errors::Result;
use crate::git_integration::run_git_captured;
use crate::stream::stdout_process_stream::AsyncStdoutDataIterator;
use git2::Oid;
use progress_reporting::DataProgressReporter;
use std::fmt::Debug;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

const GIT_SMUDGE_DATA_READ_BUFFER_SIZE: usize = 64 * 1024 * 1024;

// Tracks what initialization still needs to happen.
#[derive(Default)]
pub struct RepoInitTracking {
    lfs_is_initialized: AtomicBool,
    git_lfs_init_lock: Mutex<()>,
}

/// Translate old blob contents into new blob contents.
pub async fn translate_blob_contents(
    src_repo_dir: &Path,
    pft: Arc<PointerFileTranslatorV2>,
    blob_oid: Oid,
    progress_reporting: Arc<DataProgressReporter>,
    src_data: Vec<u8>,
    repo_init_tracking: Arc<RepoInitTracking>,
) -> Result<Vec<u8>> {
    // Identify the process needed to pull the data out.
    let name = format!("BLOB:{blob_oid:?}");
    // Is it a git lfs pointer?  If so, then run git lfs to get the git lfs data.
    if is_git_lfs_pointer(&src_data[..]) {
        info!("Source blob ID {blob_oid:?} is git lfs pointer file; smudging through git-lfs.");
        ensure_git_lfs_is_initialized(src_repo_dir, &repo_init_tracking).await?;

        let git_lfs_reader = smudge_git_lfs_pointer(src_repo_dir, src_data.clone()).await?;
        let ret_data
        = pft.clean_file_and_report_progress(
            &PathBuf::from_str(&name).unwrap(),
            git_lfs_reader,
            &Some(progress_reporting),
        )
        .await.map_err(|e| {
            warn!("Error filtering git-lfs blob {name}: {e}, contents = \"{}\".  Importing as is.",
            std::str::from_utf8(&src_data[..]).unwrap_or("<Binary Data>"));
            e
        }).unwrap_or(src_data);

        Ok(ret_data)
    } else if is_xet_pointer_file(&src_data[..]) {
        info!("Source blob ID {blob_oid:?} is git xet pointer file; smudging through git-xet.");
        let git_xet_pointer = smudge_git_xet_pointer(src_repo_dir, src_data.clone()).await?;
        let ret_data = pft
            .clean_file_and_report_progress(
                &PathBuf::from_str(&name).unwrap(),
                git_xet_pointer,
                &Some(progress_reporting),
            )
            .await
            .map_err(|e| {
                warn!(
                    "Error filtering Xet pointer at {name}, contents = \"{}\".  Importing as is.",
                    std::str::from_utf8(&src_data[..]).unwrap_or("<Binary Data>")
                );
                e
            })
            .unwrap_or(src_data);

        Ok(ret_data)
    } else {
        debug!("Cleaning blob {blob_oid:?} of size {}", src_data.len());
        // Return the filtered data
        pft.clean_file_and_report_progress(
            &PathBuf::from_str(&name).unwrap(),
            src_data,
            &Some(progress_reporting),
        )
        .await
    }
}

async fn ensure_git_lfs_is_initialized(
    src_repo_dir: &Path,
    init_locks: &Arc<RepoInitTracking>,
) -> Result<()> {
    if !init_locks
        .lfs_is_initialized
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        let _lg = init_locks.git_lfs_init_lock.lock().await;

        if init_locks
            .lfs_is_initialized
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Ok(());
        }

        info!("Running git lfs install in {src_repo_dir:?}");

        run_git_captured(Some(&src_repo_dir.to_path_buf()), "lfs", &["install"], true, None).map_err(|e| {
            error!("Error running `git lfs install` on repository with lfs pointers: {e:?}.  Please ensure git lfs is installed correctly.");
            e
        })?;

        init_locks
            .lfs_is_initialized
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    Ok(())
}

pub fn is_git_lfs_pointer(data: &[u8]) -> bool {
    if data.len() >= 1024 {
        return false;
    }

    // Convert &[u8] to a &str to parse as text.
    let Ok(text) = std::str::from_utf8(data) else {
        return false;
    };

    let text = text.trim_start();

    text.starts_with("version https://git-lfs.github.com/spec/v1") && text.contains("oid sha256:")
}

pub async fn smudge_git_lfs_pointer(
    repo_dir: impl AsRef<Path> + Debug,
    data: Vec<u8>,
) -> Result<AsyncStdoutDataIterator> {
    // Spawn a command with the necessary setup
    let mut command = tokio::process::Command::new("git");

    command.current_dir(repo_dir).arg("lfs").arg("smudge");

    AsyncStdoutDataIterator::from_command(command, &data[..], GIT_SMUDGE_DATA_READ_BUFFER_SIZE)
        .await
}

pub async fn smudge_git_xet_pointer(
    repo_dir: impl AsRef<Path> + Debug,
    data: Vec<u8>,
) -> Result<AsyncStdoutDataIterator> {
    // Spawn a command with the necessary setup
    let mut command = tokio::process::Command::new("git");

    command.current_dir(repo_dir).arg("xet").arg("smudge");

    AsyncStdoutDataIterator::from_command(command, &data[..], GIT_SMUDGE_DATA_READ_BUFFER_SIZE)
        .await
}
