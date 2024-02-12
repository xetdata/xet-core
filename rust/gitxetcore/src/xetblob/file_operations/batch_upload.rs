use super::batch_operations::BatchedRepoOperation;
use crate::errors::*;
use progress_reporting::DataProgressReporter;
use std::sync::Arc;
use tokio::task::JoinSet;

pub async fn upload_single_file(
    batch_mng: Arc<BatchedRepoOperation>,
    src_path: String,
    dest_path: String,
    progress_reporter: Arc<DataProgressReporter>,
) -> Result<()> {
    batch_mng.upload_file()?;

    Ok(())
}

pub async fn upload_all(
    batch_mng: Arc<BatchedRepoOperation>,
    src_list: &[String],
    dest_path: String,
    recursive: bool,
    progress_reporter: Arc<DataProgressReporter>,
) -> Result<()> {
    let task_join_handle = JoinSet::<Result<()>>::new();

    /// Go through
    Ok(())
}
