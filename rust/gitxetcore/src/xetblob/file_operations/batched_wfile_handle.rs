use super::batch_operations::WriteTransactionToken;
use crate::errors::Result;
use crate::xetblob::*;
use std::{io::Read, path::Path, sync::Arc};

const WRITE_FILE_BLOCK_SIZE: usize = 16 * 1024 * 1024;

pub struct BatchedWriteFileHandle {
    pub(super) writer: Arc<XetWFileObject>,
    pub(super) transaction_write_token: WriteTransactionToken,
}

impl BatchedWriteFileHandle {
    pub async fn is_closed(&self) -> bool {
        self.writer.is_closed().await
    }
    pub async fn close(mut self) -> Result<()> {
        self.writer.close().await?;

        // Give back the handle explicitly in order to ensure errors
        // can get propegated properly here.
        self.transaction_write_token.release().await
    }

    pub async fn write(&mut self, b: &[u8]) -> Result<()> {
        if self
            .transaction_write_token
            .access_transaction_for_read()
            .await?
            .commit_canceled
        {
            Err(anyhow!("Write terminated as transaction was canceled."))?;
        }

        Ok(self.writer.write(b).await?)
    }

    /// Loads data from a file into this handle to the remote file writer.
    pub async fn upload_from_file_and_close(mut self, path: impl AsRef<Path>) -> Result<()> {
        let mut file = std::fs::File::open(path)?;

        let mut buffer = vec![0u8; WRITE_FILE_BLOCK_SIZE];

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
