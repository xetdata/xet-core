use crate::errors::{GitXetRepoError, Result};
use crate::xetblob::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{OwnedRwLockWriteGuard, RwLock, Semaphore};
use tracing::{debug, error, info};

const MAX_NUM_CONCURRENT_TRANSACTIONS: usize = 3;

// Set the transaction size.
static TRANSACTION_LIMIT_LOCK: Semaphore = Semaphore::const_new(MAX_NUM_CONCURRENT_TRANSACTIONS);

pub struct WriteTransactionImpl {
    transaction: Option<XetRepoWriteTransaction>,
    branch: String,

    pub new_files: Vec<String>,
    pub copies: Vec<(String, String)>,
    pub deletes: Vec<String>,
    pub moves: Vec<(String, String)>,

    // A transaction will be cancelled on completion unless
    // this flag is set.
    commit_when_ready: bool,

    // For testing: generate an error on the commit to make sure everything works
    // above this.
    error_on_commit: bool,

    // For testing, things succeed but don't actually push.
    do_not_commit: bool,

    // For testing: go through everything, but don't actually do the last bit of the commit.
    pub commit_canceled: bool,

    // The message written on success
    commit_message: String,
    _transaction_permit: tokio::sync::SemaphorePermit<'static>,

    // Number of events
    num_events: AtomicUsize,
}

pub type TransactionHandle = Arc<RwLock<WriteTransactionImpl>>;

impl WriteTransactionImpl {
    pub async fn new(
        repo: &Arc<XetRepo>,
        branch: &str,
        commit_message: &str,
    ) -> Result<Arc<RwLock<Self>>> {
        let transaction = repo.begin_write_transaction(branch, None, None).await?;

        debug!("WriteTransaction::new_transaction(): Acquiring transaction permit.");
        let transaction_permit = TRANSACTION_LIMIT_LOCK.acquire().await.unwrap();

        let write_transaction = Self {
            transaction: Some(transaction),
            branch: branch.to_string(),
            commit_message: commit_message.to_string(),
            copies: Vec::new(),
            deletes: Vec::new(),
            moves: Vec::new(),
            new_files: Vec::new(),
            do_not_commit: false,
            error_on_commit: false,
            commit_when_ready: false,
            _transaction_permit: transaction_permit,
            commit_canceled: false,
            num_events: AtomicUsize::new(0),
        };

        Ok(Arc::new(RwLock::new(write_transaction)))
    }

    /// Complete the transaction by either cancelling it or committing it, depending on flags.
    pub async fn complete(&mut self) -> Result<()> {
        if let Some(transaction) = self.transaction.take() {
            if self.error_on_commit {
                Err(anyhow!("Error on commit flagged; Cancelling transaction."))?;
                unreachable!();
            }

            if self.commit_canceled || !self.commit_when_ready {
                info!("WriteTransactionInternal: Cancelling commit.");
                transaction.cancel().await?;
            } else if !self.do_not_commit {
                transaction.commit(&self.commit_message).await?;
            }
        }

        Ok(())
    }

    pub fn set_commit_when_ready(&mut self) {
        if !self.do_not_commit {
            self.commit_when_ready = true;
        }
    }

    pub fn set_cancel_flag(&mut self) {
        self.commit_canceled = true;
    }

    /// This is for testing
    pub fn set_do_not_commit(&mut self) {
        self.do_not_commit = true;
    }

    /// This is for testing
    pub fn set_error_on_commit(&mut self) {
        self.error_on_commit = true;
    }

    pub async fn open_for_write(&mut self, path: &str) -> Result<Arc<XetWFileObject>> {
        if self.commit_canceled {
            // No point doing anything more.
            error!("open_for_write failed: Transaction has been canceled.");
            return Err(GitXetRepoError::InternalError(anyhow!(
                "open_for_write failed: Transaction has been canceled."
            )));
        }

        if let Some(transaction) = &mut self.transaction {
            self.new_files
                .push(format!("{}/{path}", self.branch).to_string());
            let writer = transaction.open_for_write(path).await?;
            Ok(writer)
        } else {
            error!("open_for_write called after transaction completed.");
            Err(GitXetRepoError::InternalError(anyhow!(
                "open_for_write called after transaction completed."
            )))
        }
    }

    pub async fn transaction_size(&self) -> Result<usize> {
        if let Some(transaction) = &self.transaction {
            Ok(transaction.transaction_size().await)
        } else {
            error!("transaction_size called after transaction completed.");
            Err(GitXetRepoError::InternalError(anyhow!(
                "transaction_size called after transaction completed."
            )))
        }
    }

    pub async fn delete(&mut self, path: &str) -> Result<()> {
        if self.commit_canceled {
            error!("delete failed: Transaction has been canceled.");
            // No point doing anything more.
            return Err(GitXetRepoError::InternalError(anyhow!(
                "delete failed: Transaction has been canceled."
            )));
        }

        if let Some(transaction) = &mut self.transaction {
            debug!("Deleting {path}");
            transaction.delete(path).await?;

            self.deletes
                .push(format!("{}/{path}", self.branch).to_string());
            Ok(())
        } else {
            Err(GitXetRepoError::InternalError(anyhow!(
                "delete called after transaction completed."
            )))
        }
    }

    /// Copies a file from a possibly different branch into this current location.
    pub async fn copy_within_repo(
        &mut self,
        src_branch: &str,
        src_path: &str,
        target_path: &str,
    ) -> Result<()> {
        if self.commit_canceled {
            // No point doing anything more.
            error!("copy failed: Transaction has been canceled.");
            return Err(GitXetRepoError::InternalError(anyhow!(
                "copy failed: Transaction has been canceled."
            )));
        }

        if let Some(transaction) = &mut self.transaction {
            transaction.copy(src_branch, src_path, target_path).await?;
            self.copies.push((
                format!("{src_branch}/{src_path}").to_string(),
                format!("{}/{target_path}", self.branch).to_string(),
            ));
            Ok(())
        } else {
            Err(GitXetRepoError::InternalError(anyhow!(
                "copy called after transaction completed."
            )))
        }
    }

    pub async fn move_within_branch(&mut self, src_path: &str, target_path: &str) -> Result<()> {
        if self.commit_canceled {
            // No point doing anything more.
            error!("mv failed: Transaction has been canceled.");
            return Err(GitXetRepoError::InternalError(anyhow!(
                "mv failed: Transaction has been canceled."
            )));
        }

        if let Some(transaction) = &mut self.transaction {
            transaction.mv(src_path, target_path).await?;
            self.moves.push((
                format!("{}/{src_path}", self.branch).to_string(),
                format!("{}/{target_path}", self.branch).to_string(),
            ));
            Ok(())
        } else {
            Err(GitXetRepoError::InternalError(anyhow!(
                "copy called after transaction completed."
            )))
        }
    }

    pub fn action_counter(&self, increment: bool) -> usize {
        if increment {
            self.num_events.fetch_add(1, Ordering::SeqCst)
        } else {
            self.num_events.load(Ordering::Acquire)
        }
    }

    // Deregister a writer on a successful close by forcing that writer to give
    // back the transaction writing permit.  This allows for proper error propagation
    // when calling close() while ensuring that all combinations of two pathways
    // (Drop or explicit close) to closing a writing never leave the transaction in a
    // bad state.
    pub async fn complete_if_last(handle: TransactionHandle) -> Result<()> {
        // Only shut down if this is the last reference to self.  This works only if this is the
        // only reference to the PyWriteTransactionInternal
        // object.

        if let Some(s) = Arc::<_>::into_inner(handle) {
            s.into_inner().complete().await?;
        }
        Ok(())
    }

    pub async fn execute_operation<E, Fut>(
        tr: TransactionHandle,
        f: impl Fn(OwnedRwLockWriteGuard<WriteTransactionImpl>) -> Fut,
    ) -> Result<()>
    where
        E: std::fmt::Debug,
        GitXetRepoError: From<E>,
        Fut: std::future::Future<Output = std::result::Result<(), E>>,
    {
        let ret = {
            let tr_write = tr.clone().write_owned().await;
            f(tr_write).await
        };

        match ret {
            Ok(_) => {
                WriteTransactionImpl::complete_if_last(tr).await?;
            }
            Err(e) => {
                error!("Error encountered attempting batched operation: {e:?}");
                tr.write().await.set_cancel_flag();
                WriteTransactionImpl::complete_if_last(tr).await?;
                Err(e)?;
            }
        };
        Ok(())
    }
}
