use super::utils::xet_join;
use std::io;
use std::path::Path;
use std::sync::Arc;

use walkdir::WalkDir;

use crate::errors::Result;
use crate::xetblob::XetRepoOperationBatch;

fn perform_upload(
    fs_dest: Arc<XetRepoOperationBatch>,
    sources: &[impl AsRef<Path>],
    dest: &str,
    recursive: bool,
) -> Result<()> {
    for source in sources {
        let source = source.as_ref();
        if source.is_dir() {
            if recursive {
                copy_dir_recursively(fs_dest.clone(), source, dest)?;
            } else {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Attempting to copy directory {source:?} non-recursively.",),
                ))?;
            }
        } else {
            let dest_file_path = xet_join(dest, source);
            fs_dest.upload(source, &dest_file_path)?;
        }
    }
    Ok(())
}

fn copy_dir_recursively(
    fs_dest: Arc<XetRepoOperationBatch>,
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
