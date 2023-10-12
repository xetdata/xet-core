use async_trait::async_trait;
use merkledb::AsyncIterator;
use std::io;
/// Wraps a Reader and converts to an AsyncFileIterator
pub struct AsyncFileIterator<T: io::Read> {
    reader: T,
    bufsize: usize,
}

impl<T: io::Read + Send + Sync> AsyncFileIterator<T> {
    /// Constructs an AsyncFileIterator reader with an internal
    /// buffer size. It is not guaranteed that every read will
    /// fill the complete buffer.
    pub fn new(reader: T, bufsize: usize) -> Self {
        Self { reader, bufsize }
    }
}

#[async_trait]
impl<T: io::Read + Send + Sync> AsyncIterator for AsyncFileIterator<T> {
    async fn next(&mut self) -> io::Result<Option<Vec<u8>>> {
        let mut buffer = vec![0u8 ; self.bufsize]; 
        let readlen = self.reader.read(&mut buffer)?;
        if readlen > 0 {
            buffer.resize(readlen, 0);
            Ok(Some(buffer))
        } else {
            Ok(None)
        }
    }
}
