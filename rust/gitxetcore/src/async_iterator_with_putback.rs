use async_trait::async_trait;
use merkledb::*;
use std::io;
use std::io::{Cursor, Seek, SeekFrom, Write};

/// An AsyncIterator that wraps another AsyncIterator adding a "putback" method
/// that allows bytes to returned to the stream. Future reads will first consume
/// the "put-backed" bytes before reading the original stream.
/// The putback is in FIFO order. i.e. the first bytes that are put back into
/// the buffer will be first bytes that are read.
///
/// ```ignore
/// a = read from iterator
/// b = read from iterator
/// iterator.putback(a);
/// iterator.putback(b);
/// r = read from iterator // this will be bytes from "a"
pub struct AsyncIteratorWithPutBack<T: AsyncIterator + Send + Sync> {
    iterator: T,
    putback_buffer: Cursor<Vec<u8>>,
}

impl<T: AsyncIterator + Send + Sync> AsyncIteratorWithPutBack<T> {
    pub fn new(iterator: T) -> AsyncIteratorWithPutBack<T> {
        AsyncIteratorWithPutBack {
            iterator,
            putback_buffer: Cursor::new(Vec::new()),
        }
    }
    /// Puts back a collection of bytes back into the stream
    pub fn putback(&mut self, v: &[u8]) {
        self.putback_buffer.seek(SeekFrom::End(0)).unwrap();
        self.putback_buffer.write_all(v).unwrap();
    }
    pub fn peek_putback(&mut self) -> &[u8] {
        self.putback_buffer.get_ref()
    }
    pub fn flush_putback(&mut self) -> Vec<u8> {
        let ret = std::mem::replace(&mut self.putback_buffer, Cursor::new(Vec::new()));
        ret.into_inner()
    }
}

#[async_trait]
impl<T: AsyncIterator + Send + Sync> AsyncIterator for AsyncIteratorWithPutBack<T> {
    async fn next(&mut self) -> io::Result<Option<Vec<u8>>> {
        if self.putback_buffer.get_ref().is_empty() {
            self.iterator.next().await
        } else {
            let ret = std::mem::replace(&mut self.putback_buffer, Cursor::new(Vec::new()));
            Ok(Some(ret.into_inner()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    struct AsyncCursor {
        buf: Cursor<Vec<u8>>,
    }
    #[async_trait]
    impl AsyncIterator for AsyncCursor {
        async fn next(&mut self) -> io::Result<Option<Vec<u8>>> {
            // we read 8 bytes at a time
            let mut buffer = [0; 8];
            let n = self.buf.read(&mut buffer[..])?;
            if n > 0 {
                Ok(Some(buffer[..n].to_vec()))
            } else {
                Ok(None)
            }
        }
    }

    #[tokio::test]
    async fn test_async_putback_iterator() {
        let initial_buf = "123456782345678934567890123".as_bytes().to_vec();
        let iter = AsyncCursor {
            buf: std::io::Cursor::new(initial_buf),
        };
        let mut iter = AsyncIteratorWithPutBack::new(iter);

        let r1 = iter.next().await.unwrap().unwrap();
        assert_eq!(std::str::from_utf8(&r1).unwrap(), "12345678");
        let r2 = iter.next().await.unwrap().unwrap();
        assert_eq!(std::str::from_utf8(&r2).unwrap(), "23456789");
        // putback is FIFO
        iter.putback(&r1);
        iter.putback(&r2);

        // peek the stream
        // which should be r1 r2 concatenated
        assert_eq!(
            std::str::from_utf8(iter.peek_putback()).unwrap(),
            "1234567823456789"
        );
        // next read should flush the putback buffer
        let r = iter.next().await.unwrap().unwrap();
        assert_eq!(std::str::from_utf8(&r).unwrap(), "1234567823456789");

        // next read should come from the underlying buffer
        let r = iter.next().await.unwrap().unwrap();
        assert_eq!(std::str::from_utf8(&r).unwrap(), "34567890");

        // put back and read again. Make sure the
        // putback buffer is "reuseable"
        iter.putback(&r1);
        let r = iter.next().await.unwrap().unwrap();
        assert_eq!(std::str::from_utf8(&r).unwrap(), "12345678");

        // final read
        let r = iter.next().await.unwrap().unwrap();
        assert_eq!(std::str::from_utf8(&r).unwrap(), "123");

        // eof
        let r = iter.next().await.unwrap();
        assert!(r.is_none())
    }
}
