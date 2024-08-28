use std::fmt::Debug;
use std::ops::Range;

use async_trait::async_trait;

use cache::Remote;
use cas::key::Key;

use crate::interface::Client;

#[derive(Debug)]
pub struct ClientRemoteAdapter<T: Client + Debug> {
    client: T,
}
impl<T: Client + Debug> ClientRemoteAdapter<T> {
    pub fn new(client: T) -> ClientRemoteAdapter<T> {
        ClientRemoteAdapter { client }
    }
}

#[async_trait(? Send)]
impl<T: Client + Debug> Remote for ClientRemoteAdapter<T> {
    /// Fetches the provided range from the backing storage, returning the contents
    /// if they are present.
    async fn fetch(
        &self,
        key: &Key,
        range: Range<u64>,
    ) -> std::result::Result<Vec<u8>, anyhow::Error> {
        Ok(self
            .client
            .get_object_range(&key.prefix, &key.hash, vec![(range.start, range.end)])
            .await
            .map(|mut v| v.swap_remove(0))?)
    }
}
