use async_trait::async_trait;
use std::io::Read;
use std::marker::PhantomData;

use crate::errors::{GitXetRepoError, Result};
use parutils::AsyncIterator;

pub trait AsyncDataIterator: AsyncIterator<GitXetRepoError, Item = Vec<u8>> {}

pub struct AsyncDataIteratorObj<It: AsyncIterator<E>, E: Send + Sync + 'static>
where
    It::Item: Into<Vec<u8>>,
    E: Into<GitXetRepoError>,
{
    inner: It,
    _e: PhantomData<E>,
}

#[async_trait]
impl<E: Send + Sync + 'static, It: AsyncIterator<E>> AsyncIterator<GitXetRepoError>
    for AsyncDataIteratorObj<It, E>
where
    It::Item: Into<Vec<u8>>,
    GitXetRepoError: From<E>,
{
    type Item = Vec<u8>;

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        Ok(self.inner.next().await?.map(|v| v.into()))
    }
}
impl<E: Send + Sync + 'static, It: AsyncIterator<E>> AsyncDataIterator
    for AsyncDataIteratorObj<It, E>
where
    It::Item: Into<Vec<u8>>,
    GitXetRepoError: From<E>,
{
}

pub trait AsyncDataIteratorConvertable<E: Send + Sync + 'static>: AsyncIterator<E> + Sized
where
    Self::Item: Into<Vec<u8>>,
    GitXetRepoError: From<E>,
{
    fn data_iter(self) -> AsyncDataIteratorObj<Self, E>;
}

impl<A, E: Send + Sync + 'static> AsyncDataIteratorConvertable<E> for A
where
    A: AsyncIterator<E> + Sized,
    A::Item: Into<Vec<u8>>,
    GitXetRepoError: From<E>,
{
    fn data_iter(self) -> AsyncDataIteratorObj<Self, E> {
        AsyncDataIteratorObj {
            inner: self,
            _e: Default::default(),
        }
    }
}

/// Wraps a Reader and converts to an AsyncFileIterator
pub struct AsyncFileIterator<T: Read + Send + Sync> {
    reader: T,
    bufsize: usize,
}

impl<T: Read + Send + Sync> AsyncFileIterator<T> {
    /// Constructs an AsyncFileIterator reader with an internal
    /// buffer size. It is not guaranteed that every read will
    /// fill the complete buffer.
    pub fn new(reader: T, bufsize: usize) -> Self {
        Self { reader, bufsize }
    }
}

#[async_trait]
impl<T: Read + Send + Sync> AsyncIterator<GitXetRepoError> for AsyncFileIterator<T> {
    type Item = Vec<u8>;

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        let mut buffer = vec![0u8; self.bufsize];
        let readlen = self.reader.read(&mut buffer)?;
        if readlen > 0 {
            buffer.resize(readlen, 0);
            Ok(Some(buffer))
        } else {
            Ok(None)
        }
    }
}

impl<T: Read + Send + Sync> AsyncDataIterator for AsyncFileIterator<T> {}
