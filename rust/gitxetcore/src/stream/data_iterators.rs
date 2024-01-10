use async_trait::async_trait;
use std::io::Read;
use std::marker::PhantomData;

use crate::errors::{GitXetRepoError, Result};
use parutils::AsyncIterator;

/// Create a trait that standardizes the AsyncIterator to use GitXetRepoError as the error type
/// and Vec<u8> as the payload.  This is used frequently in a lot of the different processes.
pub trait AsyncDataIterator: AsyncIterator<GitXetRepoError, Item = Vec<u8>> {}

/// Many of the places that take an AsyncDataIterator are called with a different iterator.  
/// This class wraps a previous iterator to allow different errors to be properly propegated
/// using ?.   
///
/// The main way this struct is created is using the .data_iter() method off of any
/// AsyncIterator such that E is convertable to GitXetRepoError and the Item type
/// supports Into<Vec<u8>>.
///
/// E.g. my_func(g.data_iter())
///
pub struct AsyncDataIteratorObj<It: AsyncIterator<E>, E: Send + Sync + 'static>
where
    It::Item: Into<Vec<u8>>,
    E: Into<GitXetRepoError>,
{
    inner: It,
    _e: PhantomData<E>,
}

/// AsyncIterator Trait implementation of above object.
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

/// A wrapper around a Read object that implements the AsyncDataIterator trait.  
/// Useful for piping files through objects expecting the AsyncDataIterator or similar traits.
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
