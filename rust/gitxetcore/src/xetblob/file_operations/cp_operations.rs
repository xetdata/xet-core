use git2::build;
use tracing::info;

use super::fs_interface::{FSInterface, LocalFSHandle, XetFSHandle};
use super::utils::xet_join;
use std::sync::Arc;

use crate::errors::{GitXetRepoError, Result};
use crate::git_integration::git_url::parse_remote_url;
use crate::xetblob::{XetRepo, XetRepoManager, XetRepoOperationBatch};

struct CPOperation {
    src_path: String,
    dest_dir: String,
    dest_path: String,
    size: Option<u64>,
}

struct XetRepoUploader {
    repo: Arc<XetRepo>,
    fs_dest: XetRepoOperationBatch,
}

enum DestType {
    Directory,
    File,
    NonExistent,
}

async fn build_cp_operation_list(
    src_fs: Arc<dyn FSInterface>,
    sources: &[String],
    dest_fs: Arc<dyn FSInterface>,
    dest_path: String,
    recursive: bool,
) -> Result<Vec<CPOperation>> {
    let mut cp_ops = Vec::new();

    let dest_specified_as_directory = dest_path.ends_with('/');
    let dest_base_path = dest_path.strip_suffix('/').unwrap_or(dest_path.as_str());

    let dest_base_info = dest_fs.info(dest_base_path).await?;

    // Now, determine the type of the destination:
    let dest_type = {
        if let Some(dest_info) = dest_fs.info(dest_base_path).await? {
            if dest_info.is_dir_or_branch() {
                DestType::Directory
            } else {
                DestType::File
            }
        } else {
            DestType::NonExistent
        }
    };

    for src in sources {
        let src_is_directory;
        let src_path: String;

        if src.as_os_str().contains('*') {
            return Err(GitXetRepoError::InvalidOperation(
                "Error: Currently, wildcards are not supported.".to_owned(),
            ));
        }

        // Validate that the source path is specified correctly, and set src_is_directory.
        let Some(src_info) = src_fs.info(src).await? else {
            return Err(GitXetRepoError::InvalidOperation(
                "Error: Source {src_path} does not exist".to_owned(),
            ));
        };

        if src.ends_with('/') {
            src_path = src.strip_suffix('/').unwrap_or(src.as_str()).to_owned();

            if !src_info.is_dir_or_branch() {
                return Err(GitXetRepoError::InvalidOperation(format!(
                    "Source path {src} does not exist as a directory."
                )));
            }

            src_is_directory = true;
        } else {
            src_is_directory = src_info.is_dir_or_branch();
            src_path = src;
        }

        let dest_dir: String;
        let dest_path: String;

        match dest_type {
            DestType::Directory => {
                let end_component = src_fs.file_name(&src_path);
                if src_is_directory {
                    dest_dir = dest_fs.join(dest_base_path, end_component);
                    dest_path = "".to_owned();
                } else {
                    dest_path = dest_fs.join(dest_base_path, end_component);
                    dest_dir = dest_base_path.to_owned();
                }
            }
            DestType::File => {
                if src_is_directory {
                    return Err(GitXetRepoError::InvalidOperation(format!(
                        "Copy: source {src} is a directory, but destination {dest_base_path} is a file."
                    )));
                } else {
                    dest_dir = dest_fs.parent(dest_base_path).to_owned();
                    dest_path = dest_base_path.to_owned();
                }
            }
            DestType::NonExistent => {
                if src_is_directory {
                    dest_dir = &dest_base_path;
                } else if dest_specified_as_directory {
                    // Slightly different behavior depending on whether the destination is specified as
                    // a directory or not.  If it is, then copy the source file into the dest path, otherwise
                    // copy the source to the dest path name.

                    // git-xet cp dir/subdir/file dest/d1/  -> goes into dest/d1/file

                    dest_dir = dest_base_path.to_owned();
                    let end_component = src_fs.file_name(&src_path);
                    dest_path = dest_fs.join(&dest_dir, end_component);
                } else {
                    // git-xet cp dir/subdir/file dest/d1/  -> goes into dest/d1/file
                    dest_dir = dest_fs.parent(dest_base_path).to_owned();
                    dest_path = dest_base_path.to_owned();
                }
            }
        };

        // Now, we should have all the variables -- src_is_directory, src_path, dest_path, dest_dir, and file_filter_pattern
        // set up correctly.  The easiest way to break this up is between the multiple file case (src_is_directory = True) and the
        // single file case.

        if src_is_directory {
            // With the source a directory, we need to list out all the files to copy.
            if recursive {
                // If both the source and the dest are xet, then
                if src_fs.is_xet() && dest_fs.is_xet() {
                    // In this case, this can be just one operation.
                    cp_ops.push(CPOperation {
                        src_path,
                        dest_dir,
                        dest_path,
                        size: src_info.size.unwrap_or(0),
                    });
                } else {
                    cp_ops.extend(src_fs.listdir(&src_path, true).into_iter().map(|entry| {
                        let cp_src_path = src_fs.join(&src_path, &entry.name);
                        let cp_dest_path = dest_fs.join(&dest_path, &entry.name).to_owned();
                        let cp_dest_dir = dest_fs
                            .parent(&cp_dest_path)
                            .unwrap_or(&dest_dir)
                            .to_owned();

                        CPOperation {
                            src_path: cp_src_path,
                            dest_path: cp_dest_path,
                            dest_dir: cp_dest_dir,
                            size: Some(entry.size),
                        }
                    }));
                }
            } else {
                return Err(GitXetRepoError::InvalidOperation(format!(
                    "Copy: source {src} is a directory, but cp not recursive."
                )));
            }
        } else {
            cp_ops.push(CPOperation {
                src_path,
                dest_dir,
                dest_path,
                size: src_info.size.unwrap_or(0),
            });
        }
    }

    Ok(cp_ops)
}

pub async fn perform_copy(
    repo_manager: Arc<XetRepoManager>,
    sources: &[String],
    raw_dest: String,
    recursive: bool,
) -> Result<()> {
    for s in sources {
        if s.starts_with("xet://") {
            return Err(GitXetRepoError::InvalidOperation(format!(
                "Only copy from local to remote is currently supported."
            )));
        }
    }

    let src_fs = Arc::new(LocalFSHandle {});
    let dest_fs;
    let dest_repo = None;
    let dest_path;

    if raw_dest.starts_with("xet://") {
        let (repo_name, branch, dest_path_) = parse_remote_url(&raw_dest)?;
        dest_path = dest_path_.unwrap_or("/".to_owned());

        let repo = repo_manager.get_repo(None, &repo_name).await?;
        let dest_repo = Some(repo.clone());

        dest_fs = XetFSHandle { repo, branch };
    } else {
        // Useful for testing.
        dest_fs = Arc::new(LocalFSHandle {});
        dest_path = raw_dest.clone();
    }

    let files = build_cp_operation_list(src_fs, sources, dest_fs, dest_path, recursive).await?;

    info!("Copying {} files to {raw_dest}", files.len());

    Ok(())
}
