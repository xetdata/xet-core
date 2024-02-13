use super::write_transaction_wrapper::*;
use crate::errors::Result;
use crate::xetblob::*;
use std::{path::Path, sync::Arc};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, RwLockWriteGuard};
use tracing::{error, info};

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
}

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
        })
    }

    pub fn branch(&self) -> &str {
        &self.branch
    }

    pub fn set_max_events_per_transaction(&mut self, n: usize) {
        self.max_events_per_transaction = n;
    }

    async fn access_inner(&self) -> Result<Arc<RwLock<WriteTransactionImpl>>> {
        if let Some(t) = self.pwt.read().await.as_ref() {
            Ok(t.clone())
        } else {
            // This means we've called close() on the transaction, then tried to use it.
            Err(anyhow!(
                "Transaction operation attempted after transaction completed.".to_owned(),
            ))?;
            unreachable!();
        }
    }

    async fn complete_impl<'a>(
        &'a self,
        pwt_lock: &mut RwLockWriteGuard<'a, Option<InnerTransaction>>,
        commit: bool,
        cleanup_immediately: bool,
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

            if cleanup_immediately {
                // If there is only one count, this will shut down the transaction
                // and propegate any errors
                trw.complete().await?;
            }
        }

        WriteTransactionImpl::release_write_token(tr).await?;

        Ok(())
    }

    pub async fn complete(&self, commit: bool, cleanup_immediately: bool) -> Result<()> {
        self.complete_impl(&mut self.pwt.write().await, commit, cleanup_immediately)
            .await
    }

    /// Returns an operation token that can be then used to do whatever is needed for the
    ///
    pub async fn get_operation_token(&self) -> Result<WriteTransactionToken> {
        loop {
            {
                let tr = self.access_inner().await?;

                // This isn't perfect, as we can have multiple events run this over the limit at the end, but
                // it's a soft limit to control how so I'm not worried
                if tr.read().await.action_counter(true) < self.max_events_per_transaction {
                    return Ok(WriteTransactionToken { tr: Some(tr) });
                }
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

                self.complete_impl(&mut pwt_lock, true, false).await?;

                // Create a new write transaction wrapper
                *pwt_lock = Some(
                    WriteTransactionImpl::new(&self.repo, &self.branch, &self.commit_message)
                        .await?,
                );
            }
        }
    }
}

impl Drop for XetRepoOperationBatch {
    fn drop(&mut self) {
        // This should only occurs in case of errors elsewhere, but must be cleaned up okay.

        // Will not cause issues, as this is gauranteed to be the only instance of the rw lock in existence.
        if let Some(handle) = self.pwt.blocking_write().take() {
            tokio::runtime::Handle::current().block_on(async {
                let res = WriteTransactionImpl::release_write_token(handle).await;
                if let Err(e) = res {
                    error!("Error deregistering write handle in transaction : {e:?}");
                }
            });
        }
    }
}

#[derive(Clone)]
pub struct WriteTransactionToken {
    tr: Option<Arc<RwLock<WriteTransactionImpl>>>,
}

impl WriteTransactionToken {
    pub async fn access_transaction_for_write(
        &self,
    ) -> Result<OwnedRwLockWriteGuard<WriteTransactionImpl>> {
        let Some(t) = &self.tr else {
            // This should only happen if it's been closed explicitly, then
            // access is attempted.
            Err(anyhow!(
                "Transaction accessed for write after being closed."
            ))?;
            unreachable!();
        };

        Ok(t.clone().write_owned().await)
    }

    pub async fn access_transaction_for_read(
        &self,
    ) -> Result<OwnedRwLockReadGuard<WriteTransactionImpl>> {
        let Some(t) = &self.tr else {
            // This should only happen if it's been closed explicitly, then
            // access is attempted.
            Err(anyhow!("Transaction accessed for read after being closed."))?;
            unreachable!();
        };

        Ok(t.clone().read_owned().await)
    }

    // release the handle.
    pub async fn release(&mut self) -> Result<()> {
        if let Some(handle) = self.tr.take() {
            WriteTransactionImpl::release_write_token(handle).await?;
        }
        Ok(())
    }
}

impl Drop for WriteTransactionToken {
    fn drop(&mut self) {
        // This should only occurs in case of errors elsewhere, but must be cleaned up okay.
        if let Some(handle) = self.tr.take() {
            tokio::runtime::Handle::current().block_on(async {
                let res = WriteTransactionImpl::release_write_token(handle).await;
                if let Err(e) = res {
                    error!("Error deregistering transaction write token: {e:?}");
                }
            });
        }
    }
}

impl WriteTransactionToken {
    pub async fn close(mut self) -> Result<()> {
        // This just allows errors that may happen when a transaction is committed.
        if let Some(handle) = self.tr.take() {
            WriteTransactionImpl::release_write_token(handle).await?;
        }
        Ok(())
    }

    pub async fn open_for_write(&self, dest_path: &str) -> Result<BatchedWriteFileHandle> {
        let writer = self
            .access_transaction_for_write()
            .await?
            .open_for_write(dest_path)
            .await?;

        Ok(BatchedWriteFileHandle {
            writer,
            transaction_write_token: self.clone(),
        })
    }

    pub async fn upload_file_and_close(
        self,
        source: impl AsRef<Path>,
        dest_path: &str,
    ) -> Result<()> {
        let w_file = self.open_for_write(dest_path).await?;
        w_file.upload_from_file_and_close(source).await?;
        self.close().await
    }

    pub async fn delete(self, path: &str) -> Result<()> {
        Ok(self
            .access_transaction_for_write()
            .await?
            .delete(path)
            .await?)
    }

    pub async fn copy_within_repo(
        self,
        src_branch: &str,
        src_path: &str,
        target_path: &str,
    ) -> Result<()> {
        Ok(self
            .access_transaction_for_write()
            .await?
            .copy_within_repo(src_branch, src_path, target_path)
            .await?)
    }

    pub async fn move_within_branch(self, src_path: &str, target_path: &str) -> Result<()> {
        Ok(self
            .access_transaction_for_write()
            .await?
            .move_within_branch(src_path, target_path)
            .await?)
    }
}
