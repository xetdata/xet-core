use super::batch_operations::WriteTransactionHandle;
use crate::errors::Result;
use crate::xetblob::*;
use lazy_static::lazy_static;
use std::{io::Read, path::Path, sync::Arc};
use tokio::sync::Semaphore;

const MAX_CONCURRENT_OPEN_WRITE_FILES: usize = 64;
const WRITE_FILE_BLOCK_SIZE: usize = 32 * 1024 * 1024;

lazy_static! {
    static ref WRITE_FILE_OPEN_PERMIT: Semaphore = Semaphore::new(MAX_CONCURRENT_OPEN_WRITE_FILES);
}

pub struct WFileHandle {
    pub(super) writer: Arc<XetWFileObject>,
    pub(super) transaction_write_handle: WriteTransactionHandle,
}

impl WFileHandle {
    pub async fn is_closed(&self) -> bool {
        self.writer.is_closed().await
    }
    pub async fn close(mut self) -> Result<()> {
        self.writer.close().await?;

        // Give back the handle explicitly in order to ensure errors
        // can get propegated properly here.
        self.transaction_write_handle.release().await
    }

    pub async fn write(&mut self, b: &[u8]) -> Result<()> {
        if self
            .transaction_write_handle
            .access_transaction_for_read()
            .await?
            .commit_canceled
        {
            Err(anyhow!("Write terminated as transaction was canceled."))?;
        }

        Ok(self.writer.write(b).await?)
    }

    /// Loads data from a file into this.
    pub async fn upload_from_file_and_close(mut self, path: impl AsRef<Path>) -> Result<()> {
        // Get the permit to open this and write with it.  We just don't want to exceed the number of files on upload.
        let _write_file_open_permit = WRITE_FILE_OPEN_PERMIT.acquire().await.unwrap();

        let mut file = std::fs::File::open(path)?;

        let mut buffer = [0u8; WRITE_FILE_BLOCK_SIZE];

        loop {
            let n_bytes = file.read(&mut buffer[..])?;

            if n_bytes == 0 {
                break;
            }

            self.write(&buffer[..n_bytes]).await?;
        }

        self.close().await?;
        Ok(())
    }

    pub fn readable(&self) -> bool {
        false
    }

    pub fn seekable(&self) -> bool {
        false
    }

    pub fn writable(&self) -> bool {
        true
    }
}
