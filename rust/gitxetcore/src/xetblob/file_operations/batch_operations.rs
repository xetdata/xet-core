// // use progress_reporting::DataProgressReporter;
// use std::io::Read;
// use std::path::Path;
// use std::sync::Arc;
// 
// use anyhow::anyhow;
// use tokio::task::JoinSet;
// use tracing::{debug, error};
// 
// use crate::errors::Result;
// use crate::xetblob::*;
// 
// use super::write_transaction_wrapper::*;
// 
// const DEFAULT_MAX_EVENTS_PER_TRANSACTION: usize = 512;
// 
// // This functions as a reference to access the internal transaction object.
// // The WriteTransaction holds one handle, and each file open for writing
// // holds one handle. Once all references are finished, then the transaction
// // is either committed or canceled.
// //
// // The lock inside is an RwLock object so that other objects can quickly check
// // whether a transaction has been canceled, allowing errors to propegate correctly.
// //
// // There are two paths for handles being released: explicitly, with proper error handling
// // and reporting, and on drop, where errors are logged and ignored.  This intermediate
// // class is needed to properly implement both semantics, so that errors on explicit
// // completion propegate properly and transactions are not left in a bad state if there are
// // errors elsewhere.
// //
// pub struct XetRepoOperationBatch {
//     pwt: Option<TransactionHandle>,
// 
//     repo: Arc<XetRepo>,
//     branch: String,
//     commit_message: String,
// 
//     max_events_per_transaction: usize,
// 
//     // Complete an operation.
//     completing_transactions: JoinSet<Result<()>>,
// }
// 
// const WRITE_FILE_BLOCK_SIZE: usize = 16 * 1024 * 1024;
// 
// impl XetRepoOperationBatch {
//     pub async fn new(repo: Arc<XetRepo>, branch: &str, commit_message: &str) -> Result<Self> {
//         Ok(Self {
//             pwt: Some(WriteTransactionImpl::new(&repo, branch, commit_message).await?),
//             repo,
//             branch: branch.to_string(),
//             commit_message: commit_message.to_string(),
//             max_events_per_transaction: DEFAULT_MAX_EVENTS_PER_TRANSACTION,
//             completing_transactions: JoinSet::new(),
//         })
//     }
// 
//     pub async fn file_upload(
//         &mut self,
//         local_path: impl AsRef<Path>,
//         remote_path: &str,
//         progress_bar: Option<Arc<DataProgressReporter>>,
//     ) -> Result<()> {
//         let tr = self.get_transaction_handle().await?;
//         let local_path = local_path.as_ref().to_path_buf();
//         let remote_path = remote_path.to_owned();
// 
//         self.completing_transactions.spawn(async move {
//             debug!("file_upload: Beginning write: {local_path:?} to {remote_path}",);
// 
//             let writer = tr.write().await.open_for_write(&remote_path).await?;
// 
//             let mut file = std::fs::File::open(&local_path)?;
//             let mut buffer = vec![0u8; WRITE_FILE_BLOCK_SIZE];
// 
//             loop {
//                 let res = file.read(&mut buffer[..]);
//                 if matches!(res, Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted) {
//                     continue;
//                 }
// 
//                 let n_bytes = res?;
// 
//                 if n_bytes == 0 {
//                     break;
//                 }
// 
//                 writer.write(&buffer[..n_bytes]).await?;
//                 if let Some(pb) = progress_bar.as_ref() {
//                     pb.register_progress(None, Some(n_bytes));
//                 }
//             }
// 
//             if let Some(pb) = progress_bar.as_ref() {
//                 pb.register_progress(Some(1), None);
//             }
// 
//             WriteTransactionImpl::complete_if_last(tr).await?;
// 
//             debug!("file_upload: write for {local_path:?} to {remote_path} completed.",);
// 
//             Ok(())
//         });
// 
//         Ok(())
//     }
// 
//     pub async fn delete(&mut self, remote_path: &str) -> Result<()> {
//         WriteTransactionImpl::execute_operation(
//             self.get_transaction_handle().await?,
//             |mut trw| async move { trw.delete(remote_path).await },
//         )
//             .await
//     }
// 
//     pub async fn copy_within_repo(
//         &mut self,
//         src_branch: &str,
//         src_path: &str,
//         target_path: &str,
//     ) -> Result<()> {
//         WriteTransactionImpl::execute_operation(
//             self.get_transaction_handle().await?,
//             |mut trw| async move {
//                 trw.copy_within_repo(src_branch, src_path, target_path)
//                     .await
//             },
//         )
//             .await
//     }
// 
//     pub async fn move_within_branch(&mut self, src_path: &str, target_path: &str) -> Result<()> {
//         WriteTransactionImpl::execute_operation(
//             self.get_transaction_handle().await?,
//             |mut trw| async move { trw.move_within_branch(src_path, target_path).await },
//         )
//             .await
//     }
// 
//     pub fn branch(&self) -> &str {
//         &self.branch
//     }
// 
//     async fn complete_impl(&mut self, commit: bool) -> Result<()> {
//         let Some(tr) = self.pwt.take() else {
//             // This means either we've called close() on the transaction, then tried to use it;
//             // or all associated write files complete and close before calling close().
//             // Either case this should be a NOP.
//             error!("Complete called twice on transaction.");
//             return Ok(());
//         };
// 
//         {
//             let mut trw = tr.write().await;
// 
//             if commit {
//                 trw.set_commit_when_ready();
//             } else {
//                 trw.set_cancel_flag();
//             }
//         }
// 
//         WriteTransactionImpl::complete_if_last(tr).await?;
// 
//         Ok(())
//     }
// 
//     pub async fn complete(&mut self, commit: bool) -> Result<()> {
//         self.complete_impl(commit).await?;
// 
//         while let Some(completion_task) = self.completing_transactions.join_next().await {
//             completion_task??;
//         }
// 
//         Ok(())
//     }
// 
//     /// Returns an operation token that can be then used to do whatever is needed for the
//     ///
//     pub async fn get_transaction_handle(&mut self) -> Result<TransactionHandle> {
//         // First, see if there are any errors in the pipeline.
//         while let Some(res) = self.completing_transactions.try_join_next() {
//             res??;
//         }
// 
//         {
//             let Some(tr) = self.pwt.as_ref().cloned() else {
//                 // This means we've called close() on the transaction, then tried to use it.
//                 Err(anyhow!(
//                     "Transaction operation attempted after transaction completed.".to_owned(),
//                 ))?;
//                 unreachable!();
//             };
// 
//             // This isn't perfect, as we can have multiple events run this over the limit at the end, but
//             // it's a soft limit to control how so I'm not worried
//             if tr.read().await.action_counter(true) < self.max_events_per_transaction {
//                 return Ok(tr.clone());
//             }
//         }
// 
//         // Commit what we currently have, then replace it.
//         self.complete_impl(true).await?;
// 
//         // Create a new write transaction and return
//         let tr = WriteTransactionImpl::new(&self.repo, &self.branch, &self.commit_message).await?;
//         self.pwt = Some(tr.clone());
//         Ok(tr)
//     }
// }
