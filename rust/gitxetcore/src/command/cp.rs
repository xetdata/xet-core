use progress_reporting::DataProgressReporter;
use tokio::task::JoinSet;
use xetblob::file_operations::BatchedRepoOperation;

use crate::git_integration::git_url::XetPathInfo;

struct CopySessionLocalToRemote {
    dpr: DataProgressReporter,
    batch_manager: Arc<BatchedRepoOperation>,
    recursive: bool,
    message: String,
}

impl CopySessionLocalToRemote {
    fn run(source_list: &[String], destination: String, recursive: bool) -> Self {
        let recursive_msg = if recursive {
            "recursively ";
        } else {
            "";
        };

        let message = if source_list.len() == 1 {
            format!(
                "Copying {} {}to {}",
                source_list[0], recursive_msg, destination
            );
        } else {
            format!(
                "Copying {} files/directories {}to {}",
                source_list.len(),
                recursive_msg,
                destination
            );
        };

        // Run the source list
    }

    fn run(&self, source_root: impl AsRef<Path>, source_path: impl AsRef<Path>) -> Result<()> {
        let source = source_root.join(source_path);

        if !source.exists() {
            return Err(FileOperationError::IllegalOperation(
                "Source file {source:?} does not exist.".to_owned(),
            ));
        }

        Ok(())
    }
}

fn perform_copy_local_to_remote(pending_tasks: &mut JoinSet, remote: &XetPathInfo) -> Result<()> {}

pub fn perform_copy(
    source_list: &[String],
    destination: String,
    recursive: bool,
) -> anyhow::Result<()> {
    if source_list.is_empty() {
        return Err(FileOperationError::InvalidArguments(
            "No source files specified.".to_owned(),
        ));
    }

    // Get the remote repo.
    let remote = XetPathInfo::parse(&destination, "")?;

    // Copy operations.

    // let progress_reporter = DataProgressReporter::new(message,

    Ok(())
}
