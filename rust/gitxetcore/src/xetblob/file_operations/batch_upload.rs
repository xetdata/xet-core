use super::utils::xet_join;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::fs_interface::XetFSInterface;
use progress_reporting::DataProgressReporter;
use tokio::task::JoinSet;
use walkdir::WalkDir;

use crate::errors::{GitXetRepoError, Result};
use crate::xetblob::{XetRepo, XetRepoOperationBatch};

struct CPOperation {
    source_path: PathBuf,
    dest_path: String,
}

struct XetRepoUploader {
    repo: Arc<XetRepo>,
    fs_dest: XetRepoOperationBatch,
    progress_bar: DataProgressReporter,
}

enum DestType {
    Directory, File, NonExistent }


impl XetRepoUploader {
    async fn upload_single_file(&mut self, source: impl AsRef<Path>, dest: &str) -> Result<()> {
        let dest_file_path = xet_join(dest, source);

    }

    async fn perform_copy_operation(
        &self,
        src_fs: impl XetFSInterface,
        sources: &[String],
        dest_fs: impl XetFSInterface,
        dest_path: String,
        recursive: bool,
    ) -> Result<()> {
        let mut dest_specified_as_directory = dest_path.ends_with('/');
        let dest_path = dest_path.strip_suffix('/').unwrap_or(dest_path.as_str());

        let dest_base_info = dest_fs.info(dest_path).await?; 


            // Now, determine the type of the destination:
            let dest_type = { 
                if let Some(dest_info) = dest_fs.info(dest_path).await? {
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
            let src_path;
            let dest_dir;
            let dest_path;

            if src.as_os_str().contains('*') {
                return Err(GitXetRepoError::InvalidOperation(
                    "Error: Currently, wildcards are not supported.".to_owned(),
                ));
            }

            // Validate that the source path is specified correctly, and set src_is_directory. 
            let Some(src_info) = src_fs.info(src_path).await? else {
                return Err(GitXetRepoError::InvalidOperation(
                    "Error: Source {src_path} does not exist".to_owned(),
                ));
            };

            if src.ends_with('/') {
                src_path = src.strip_suffix('/').unwrap_or(src.as_str());

                if !src_info.is_dir_or_branch() {
                    return Err(GitXetRepoError::InvalidOperation(format!(
                        "Source path {src} does not exist as a directory."
                    )));
                }

                src_is_directory = true;
            } else {
                src_is_directory = src_info.is_dir_or_branch();
            }

            match dest_type {
                DestType::Directory => {
                    let (_, end_component) = src_fs.split(src_path);
                    if src_is_directory { 
                        dest_dir = dest_fs.join(dest_path, end_component); 
                        dest_path = None;
                    } else {
                        dest_path =  





                },
                DestType::File => todo!,
                DestType::NonExistent => todo!(),
            }



            // First, we need to figure out the dest type -- are we copying into an existing directory or renaming the current one?
            // We only need to check if this is a single source file.
            if let Some(dest_entry) = self.repo.stat(self.fs_dest.branch(), &dest).await? {
                if sources.len() == 1 {
                    if dest_entry.is_dir_or_branch() {}
                }

                for source in sources {
                    let source = source.as_ref();

                    if source.is_dir() {
                        if recursive {
                            perform_upload_recursively(fs_dest.clone(), task_queue, source, dest)
                                .await?;
                        } else {
                            Err(io::Error::new(
                                io::ErrorKind::Other,
                                format!("Attempting to copy directory {source:?} non-recursively.",),
                            ))?;
                        }
                    } else {
                    }
                }
            }
            Ok(())
    }

        async fn perform_upload_recursively(
            fs_dest: Arc<XetRepoOperationBatch>,
            task_queue: &mut JoinSet<Result<()>>,
            source: impl AsRef<Path>,
            dest: &str,
        ) -> Result<()> {
            for entry in WalkDir::new(source).into_iter().filter_map(|e| e.ok()) {
                let path = entry.path();
                let relative_path = path.strip_prefix(source).unwrap();
                let dest_path = dest.join(relative_path);

                if path.is_dir() {
                    fs_dest.create_dir_all(&dest_path)?;
                } else {
                    if let Some(parent) = dest_path.parent() {
                        if !Path::new(parent).exists() {
                            // Assuming existence check is external or add a method in FileSystem trait
                            fs_dest.create_dir_all(parent)?;
                        }
                    }
                    fs_dest.copy(path, &dest_path)?;
                }
            }
            Ok(())
        }
    }
}
