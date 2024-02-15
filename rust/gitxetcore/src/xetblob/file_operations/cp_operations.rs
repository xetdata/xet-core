use progress_reporting::DataProgressReporter;
// use tokio::task::JoinSet;
use tracing::{info, warn};

use super::fs_interface::{FSInterface, LocalFSHandle, XetFSHandle};
use std::sync::Arc;

use crate::errors::{GitXetRepoError, Result};
use crate::git_integration::git_url::parse_remote_url;
use crate::xetblob::XetRepoManager;

struct CPOperation {
    src_path: String,
    dest_dir: String,
    dest_path: String,
    size: u64,
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

        if src.contains('*') {
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
            src_path = src.to_owned();
        }

        let dest_dir: String;
        let dest_path: String;

        match dest_type {
            DestType::Directory => {
                let end_component = src_fs.file_name(&src_path);
                if src_is_directory {
                    dest_dir = dest_fs.join(dest_base_path, end_component);
                    dest_path = dest_dir.clone();
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
                }

                dest_dir = dest_fs.parent(dest_base_path).unwrap_or("").to_owned();
                dest_path = dest_base_path.to_owned();
            }
            DestType::NonExistent => {
                if src_is_directory {
                    dest_path = dest_base_path.to_owned();
                    dest_dir = dest_fs.parent(dest_base_path).unwrap_or("").to_owned();
                } else if dest_specified_as_directory {
                    // Slightly different behavior depending on whether the destination is specified as
                    // a directory or not.  If it is, then copy the source file into the dest path, otherwise
                    // copy the source to the dest path name.

                    // git-xet cp dir/subdir/file dest/d1/  -> goes into dest/d1/file

                    dest_dir = dest_base_path.to_owned();
                    let end_component = src_fs.file_name(&src_path);
                    dest_path = dest_fs.join(&dest_dir, end_component);
                } else {
                    // git-xet cp dir/subdir/file dest/bar  -> goes into dest/bar
                    dest_dir = dest_fs.parent(dest_base_path).unwrap_or("").to_owned();
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
                        size: 0,
                    });
                } else {
                    cp_ops.extend(src_fs.listdir(&src_path, true).await?.into_iter().map(
                        |entry| {
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
                                size: entry.size,
                            }
                        },
                    ));
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
                size: src_info.size,
            });
        }
    }

    Ok(cp_ops)
}

pub async fn perform_copy(
    repo_manager: &mut XetRepoManager,
    sources: &[String],
    raw_dest: String,
    recursive: bool,
) -> Result<()> {
    for s in sources {
        if s.starts_with("xet://") {
            return Err(GitXetRepoError::InvalidOperation(format!(
                "Only copy from local to remote is currently supported ({s} invalid)."
            )));
        }
    }

    let src_fs = Arc::new(LocalFSHandle {});
    let dest_fs: Arc<dyn FSInterface>;
    let dest_repo;
    let dest_path;
    let dest_branch;

    if raw_dest.starts_with("xet://") {
        let (repo_name, branch, dest_path_) = parse_remote_url(&raw_dest)?;
        dest_path = dest_path_.unwrap_or("/".to_owned());

        let repo = repo_manager.get_repo(None, &repo_name).await?;

        dest_repo = Some(repo.clone());
        dest_branch = branch.clone();
        dest_fs = Arc::new(XetFSHandle { repo, branch });
    } else {
        // Useful for testing.
        dest_fs = Arc::new(LocalFSHandle {});
        dest_path = raw_dest.clone();
        dest_repo = None;
        dest_branch = "".to_owned();
    }

    let files = build_cp_operation_list(src_fs, sources, dest_fs, dest_path, recursive).await?;

    let mut total_size = 0;

    for cp_op in files.iter() {
        total_size += cp_op.size;
    }

    if files.is_empty() {
        warn!("No files specified; ignoring.");
        return Ok(());
    }

    // TODO: Fetch shard hint list.

    // TODO: make these a bit better.
    let (msg, commit_msg) = if files.len() == 1 {
        (
            format!("Copying {} to {raw_dest}", files[0].src_path),
            format!("Copied {} to {raw_dest}", files[0].src_path),
        )
    } else {
        (
            format!("Copying {} files to {raw_dest}", files.len()),
            format!("Copied {} files to {raw_dest}", files[0].src_path),
        )
    };
    info!("{msg}");

    let progress_bar =
        DataProgressReporter::new(&msg, Some(files.len()), Some(total_size as usize));

    // Destination is xet remote.
    if let Some(repo) = dest_repo {
        // TODO: source is remote also.

        let mut batch_mng = repo.begin_batched_write(&dest_branch, &commit_msg).await?;

        // let mut task_queue = JoinSet::<Result<()>>::new();

        for cp_op in files.into_iter() {
            let pb = progress_bar.clone();

            batch_mng
                .file_upload(cp_op.src_path, &cp_op.dest_path, Some(pb))
                .await?;
        }

        batch_mng.complete(true).await?;

        progress_bar.finalize();
    } else {
        // Destination is local file system.
        for cp_op in files.into_iter() {
            std::fs::create_dir_all(&cp_op.dest_dir)?;
            std::fs::copy(&cp_op.src_path, &cp_op.dest_path)?;
            progress_bar.register_progress(Some(1), Some(cp_op.size as usize));
        }
        progress_bar.finalize();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{distributions::Alphanumeric, Rng};
    //use std::collections::HashSet;
    use std::fs::{self, File};
    use std::io::{self, Write};
    use std::path::Path;
    use tempdir::TempDir;

    async fn perform_copy_wrapper(
        sources: &[String],
        raw_dest: String,
        recursive: bool,
    ) -> Result<()> {
        perform_copy(
            &mut XetRepoManager::new(None, None)?,
            sources,
            raw_dest,
            recursive,
        )
        .await
    }

    /// Creates a file with random content.
    fn create_random_file(path: &Path, size: usize) -> io::Result<()> {
        let mut file = File::create(path)?;
        let data: Vec<u8> = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(size)
            .collect();
        file.write_all(&data)?;
        Ok(())
    }

    /// Compares the content of two files for equality.
    fn files_are_identical(file1: &Path, file2: &Path) -> io::Result<bool> {
        let data1 = fs::read(file1)?;
        let data2 = fs::read(file2)?;
        Ok(data1 == data2)
    }

    // Utility function to create a directory structure and files for testing
    fn create_dir_structure(base: &Path, structure: &[(&str, Option<&[u8]>)]) -> io::Result<()> {
        for (path, contents) in structure {
            let full_path = base.join(path);
            if let Some(data) = contents {
                let mut file = File::create(&full_path)?;
                file.write_all(data)?;
            } else {
                fs::create_dir_all(&full_path)?;
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_single_file_to_existing_directory() {
        let temp_dir = TempDir::new("test_dir").unwrap();
        let source_dir = temp_dir.path().join("source");
        fs::create_dir(&source_dir).unwrap();
        let dest_dir = temp_dir.path().join("dest");
        fs::create_dir(&dest_dir).unwrap();

        let file_path = source_dir.join("source.txt");
        create_random_file(&file_path, 1024).unwrap();

        let sources = vec![file_path.to_str().unwrap().to_owned()];
        let destination = dest_dir.to_str().unwrap().to_owned();
        perform_copy_wrapper(&sources, destination, false)
            .await
            .unwrap();

        let dest_file_path = dest_dir.join("source.txt");
        assert!(dest_file_path.exists());
        assert!(files_are_identical(&file_path, &dest_file_path).unwrap());
    }

    #[tokio::test]
    async fn test_single_file_to_nonexistent_directory_destination() {
        let temp_dir = TempDir::new("test_dir").unwrap();
        let source_dir = temp_dir.path().join("source");
        fs::create_dir(&source_dir).unwrap();
        let file_path = source_dir.join("file.txt");
        create_random_file(&file_path, 1024).unwrap();

        let dest_dir_path = temp_dir.path().join("nonexistent_dest/");
        perform_copy_wrapper(
            &[file_path.to_str().unwrap().to_owned()],
            dest_dir_path.to_str().unwrap().to_owned(),
            false,
        )
        .await
        .unwrap();

        let dest_file_path = dest_dir_path.join("file.txt");
        assert!(dest_file_path.exists());
        assert!(files_are_identical(&file_path, &dest_file_path).unwrap());
    }

    #[tokio::test]
    async fn test_single_file_to_non_existing_file_destination() {
        let temp_dir = TempDir::new("test_dir").unwrap();
        let source_dir = temp_dir.path().join("source");
        fs::create_dir(&source_dir).unwrap();
        let file_path = source_dir.join("file.txt");
        create_random_file(&file_path, 1024).unwrap();

        let dest_file_path = temp_dir.path().join("new/dest/path/file.txt");
        perform_copy_wrapper(
            &[file_path.to_str().unwrap().to_owned()],
            dest_file_path.to_str().unwrap().to_owned(),
            false,
        )
        .await
        .unwrap();

        assert!(dest_file_path.exists());
        assert!(files_are_identical(&file_path, &dest_file_path).unwrap());
    }

    #[tokio::test]
    async fn test_directory_to_existing_directory_recursively() {
        let temp_dir = TempDir::new("test_dir").unwrap();
        let source_dir = temp_dir.path().join("source");
        let dest_dir = temp_dir.path().join("dest");
        fs::create_dir(&source_dir).unwrap();
        fs::create_dir(&dest_dir).unwrap();

        // Creating a directory structure within source_dir
        create_dir_structure(
            &source_dir,
            &[("subdir", None), ("subdir/file.txt", Some(b"Hello"))],
        )
        .unwrap();

        perform_copy_wrapper(
            &[source_dir.to_str().unwrap().to_owned()],
            dest_dir.to_str().unwrap().to_owned() + "/",
            true,
        )
        .await
        .unwrap();

        let new_source_file = dest_dir.join("source/subdir/file.txt");
        assert!(new_source_file.exists());
        assert_eq!(fs::read_to_string(new_source_file).unwrap(), "Hello");
    }

    #[tokio::test]
    async fn test_directory_to_nonexistent_directory_recursively() {
        let temp_dir = TempDir::new("test_dir").unwrap();
        let source_dir = temp_dir.path().join("source");
        fs::create_dir(&source_dir).unwrap();

        // Creating a directory structure within source_dir
        create_dir_structure(
            &source_dir,
            &[("subdir", None), ("subdir/file.txt", Some(b"Hello"))],
        )
        .unwrap();

        let dest_dir = temp_dir.path().join("nonexistent_dest");
        perform_copy_wrapper(
            &[source_dir.to_str().unwrap().to_owned()],
            dest_dir.to_str().unwrap().to_owned(),
            true,
        )
        .await
        .unwrap();

        let copied_file_path = dest_dir.join("subdir/file.txt");
        assert!(copied_file_path.exists());
        assert_eq!(fs::read_to_string(copied_file_path).unwrap(), "Hello");
    }
}
