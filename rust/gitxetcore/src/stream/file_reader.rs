use tokio::sync::mpsc::Receiver;

use async_trait::async_trait;
use merkledb::AsyncIterator;
use std::sync::atomic::{AtomicBool, Ordering};

/// Adapter between the repo manager and the asynchronous queue
/// of file objects.
pub struct FileChannelReader {
    fetch_channel: Receiver<Option<Vec<u8>>>,
    done: AtomicBool,
}

impl FileChannelReader {
    pub fn new(fetch_channel: Receiver<Option<Vec<u8>>>) -> Self {
        Self {
            fetch_channel,
            done: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl AsyncIterator for FileChannelReader {
    /// Gets the next file from the mpsc channel. Used by the repo manager
    /// to clean/smudge the files.
    async fn next(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        // we remember if we are done
        // and return OK none all the time after that.
        if self.done.load(Ordering::Relaxed) {
            return Ok(None);
        };
        match self.fetch_channel.recv().await {
            Some(x) => {
                if x.is_none() {
                    self.done.store(true, Ordering::Relaxed);
                }
                Ok(x)
            }
            None => {
                self.done.store(true, Ordering::Relaxed);
                Ok(None)
            }
        }
    }
}
