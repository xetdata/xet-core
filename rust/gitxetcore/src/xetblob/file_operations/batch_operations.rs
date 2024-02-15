use super::write_transaction_wrapper::*;
use crate::errors::Result;
use crate::xetblob::*;
use progress_reporting::DataProgressReporter;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use tokio::{
    sync::{RwLock, RwLockWriteGuard},
    task::JoinSet,
};
use tracing::info;

const DEFAULT_MAX_EVENTS_PER_TRANSACTION: usize = 512;

type InnerTransaction = Arc<RwLock<WriteTransactionImpl>>;

// This functions as a reference to access the internal transaction object.
// The WriteTransaction holds one handle, and each file open for writing
// holds one handle. Once all references are finished, then the transaction
// is either committed or canceled.
//
// The lock inside is an RwLock object so that other objects can quickly check
// whether a transaction has been canceled, allowing errors to propegate correctly.
//
// There are two paths for handles being released: explicitly, with proper error handling
// and reporting, and on drop, where errors are logged and ignored.  This intermediate
// class is needed to properly implement both semantics, so that errors on explicit
// completion propegate properly and transactions are not left in a bad state if there are
// errors elsewhere.
//
pub struct XetRepoOperationBatch {
    pwt: RwLock<Option<InnerTransaction>>,

    repo: Arc<XetRepo>,
    branch: String,
    commit_message: String,

    max_events_per_transaction: usize,

    // Complete an operation.
    completing_transactions: JoinSet<Result<()>>,
}

const WRITE_FILE_BLOCK_SIZE: usize = 16 * 1024 * 1024;

impl XetRepoOperationBatch {
    pub async fn new(repo: Arc<XetRepo>, branch: &str, commit_message: &str) -> Result<Self> {
        Ok(Self {
            pwt: RwLock::new(Some(
                WriteTransactionImpl::new(&repo, branch, commit_message).await?,
            )),
            repo,
            branch: branch.to_string(),
            commit_message: commit_message.to_string(),
            max_events_per_transaction: DEFAULT_MAX_EVENTS_PER_TRANSACTION,
            completing_transactions: JoinSet::new(),
        })
    }

    pub async fn file_upload(
        self: &mut Self,
        local_path: impl AsRef<Path>,
        remote_path: &str,
        progress_bar: Option<Arc<DataProgressReporter>>,
    ) -> Result<()> {
        let tr = self.get_operation_token().await?;
        let local_path = local_path.as_ref().to_path_buf();
        let remote_path = remote_path.to_owned();

        self.completing_transactions.spawn(async move {
            let writer = tr.write().await.open_for_write(&remote_path).await?;

            let mut file = std::fs::File::open(local_path)?;
            let mut buffer = vec![0u8; WRITE_FILE_BLOCK_SIZE];

            loop {
                let n_bytes = file.read(&mut buffer[..])?;

                if n_bytes == 0 {
                    break;
                }

                writer.write(&buffer[..n_bytes]).await?;
                if let Some(pb) = progress_bar.as_ref() {
                    pb.register_progress(None, Some(n_bytes));
                }
            }

            if let Some(pb) = progress_bar.as_ref() {
                pb.register_progress(Some(1), None);
            }

            WriteTransactionImpl::release_write_token(tr).await?;

            Ok(())
        });

        Ok(())
    }

    pub async fn delete(&mut self, remote_path: &str) -> Result<()> {
        let tr = self.get_operation_token().await?;
        tr.write().await.delete(remote_path).await?;
        WriteTransactionImpl::release_write_token(tr).await?;
        Ok(())
    }

    pub async fn copy_within_repo(
        &mut self,
        src_branch: &str,
        src_path: &str,
        target_path: &str,
    ) -> Result<()> {
        let tr = self.get_operation_token().await?;
        tr.write()
            .await
            .copy_within_repo(src_branch, src_path, target_path)
            .await?;
        WriteTransactionImpl::release_write_token(tr).await?;
        Ok(())
    }

    pub async fn move_within_branch(&mut self, src_path: &str, target_path: &str) -> Result<()> {
        let tr = self.get_operation_token().await?;
        tr.write()
            .await
            .move_within_branch(src_path, target_path)
            .await?;
        WriteTransactionImpl::release_write_token(tr).await?;
        Ok(())
    }

    pub fn branch(&self) -> &str {
        &self.branch
    }

    async fn complete_impl<'a>(
        &self,
        pwt_lock: &mut RwLockWriteGuard<'a, Option<InnerTransaction>>,
        commit: bool,
    ) -> Result<()> {
        let Some(tr) = pwt_lock.take() else {
            // This means either we've called close() on the transaction, then tried to use it;
            // or all associated write files complete and close before calling close().
            // Either case this should be a NOP.
            info!("Complete called after PyTransaction object committed");
            return Ok(());
        };

        {
            let mut trw = tr.write().await;

            if commit {
                trw.set_commit_when_ready();
            } else {
                trw.set_cancel_flag();
            }
        }

        Ok(())
    }

    pub async fn complete(&mut self, commit: bool) -> Result<()> {
        self.complete_impl(&mut self.pwt.write().await, commit)
            .await?;

        while let Some(completion_task) = self.completing_transactions.join_next().await {
            completion_task??;
        }

        Ok(())
    }

    /// Returns an operation token that can be then used to do whatever is needed for the
    ///
    pub async fn get_operation_token(&mut self) -> Result<Arc<RwLock<WriteTransactionImpl>>> {
        // First, see if there are any errors in the pipeline.
        while let Some(res) = self.completing_transactions.try_join_next() {
            res??;
        }

        loop {
            let Some(tr) = self.pwt.read().await.as_ref().map(|t| t.clone()) else {
                // This means we've called close() on the transaction, then tried to use it.
                Err(anyhow!(
                    "Transaction operation attempted after transaction completed.".to_owned(),
                ))?;
                unreachable!();
            };

            // This isn't perfect, as we can have multiple events run this over the limit at the end, but
            // it's a soft limit to control how so I'm not worried
            if tr.read().await.action_counter(true) < self.max_events_per_transaction {
                return Ok(tr.clone());
            }

            {
                // Ok, now commit and restart all of this if things haven't changed.
                let mut pwt_lock = self.pwt.write().await;

                if let Some(tr) = pwt_lock.as_ref() {
                    if tr.read().await.action_counter(false) < self.max_events_per_transaction {
                        continue;
                    }
                }

                // If this is still an issue.
                self.complete_impl(&mut pwt_lock, true).await?;

                // Create a new write transaction wrapper
                *pwt_lock = Some(
                    WriteTransactionImpl::new(&self.repo, &self.branch, &self.commit_message)
                        .await?,
                );
            }
        }
    }
}
