use std::sync::Arc;
use tokio::sync::{AcquireError, Semaphore};

#[derive(Clone)]
pub struct MemoryLimit {
    inner: Arc<Semaphore>,
}

/// Defines a memory limit of a certain number of bytes.
impl MemoryLimit {
    pub fn new(nbytes: usize) -> Self {
        Self {
            inner: Arc::new(Semaphore::new(nbytes)),
        }
    }
}

/// Suppose a graph of resource dependents that share a common limit,
/// i.e. they in total cannot hold more than N resources,
///              limit region
///         |---------------------|
///         |    A1 - ... - Z1    |
///         |  /      \ /      \  |
/// input - |  - .. - ... - .. -  |output
///         |  \      / \      /  |
///         |    An - ... - Zn    |
///         |---------------------|
/// and executors that move resources from one dependent to the next
/// (likely with some computation) that work in parallel, only the
/// executors that move resources from input to entry nodes Ai can
/// acquire tokens and executors that move resources from exits Zi to
/// output can release tokens. Otherwise deadlock may happen when any
/// participant grabs all tokens.
///                                                e1     e2     e3
/// For example with the most simple case, "input ---- A ---- B ---- output" with
/// executors e1, e2, e3 running in parallel.
/// Suppose at one point A holds all tokens (e.g. e1 being super fast or e2 delayed).
/// Now e2 removes one resource from A and releases a token, but before e2 finishing
/// processing this resource and can acquire a token to move it into B, e1 managed to
/// acquire this token. This now becomes a deadlock situation because e2 and e3 can no
/// longer make progress to move resource out of the limit region.
#[derive(Clone)]
pub struct GlobalMemoryLimit {
    inner: MemoryLimit,
    is_entry: bool,
    is_exit: bool,
}

impl GlobalMemoryLimit {
    /// For any memory resource dependent that only takes data
    /// into a region that limits memory usage.
    pub fn entry_only(limit: &MemoryLimit) -> Self {
        Self {
            inner: limit.clone(),
            is_entry: true,
            is_exit: false,
        }
    }

    /// For any memory resource dependent that only takes data
    /// out of a region that limits memory usage.
    pub fn exit_only(limit: &MemoryLimit) -> Self {
        Self {
            inner: limit.clone(),
            is_entry: false,
            is_exit: true,
        }
    }

    /// For any memory resource dependent that both takes data
    /// into and out of a region that limits memory usage.
    pub fn entry_and_exit(limit: &MemoryLimit) -> Self {
        Self {
            inner: limit.clone(),
            is_entry: true,
            is_exit: true,
        }
    }

    /// Acquire nbytes resource from this global limit.
    pub async fn acquire(&self, nbytes: impl TryInto<u32>) -> Result<(), AcquireError> {
        if self.is_entry {
            let permit = self
                .inner
                .inner
                // This function takes a u32, if a single size exceeds 4 GB, just acquire 4 GB.
                // Unfortuantely, breaking a size that is greater than 4 GB into chunks and
                // trying to acquire them in a loop will also lead to a potential deadlock,
                // if there are multiple global entries in the same limit region.
                // This will not lead to memory explotion as long as the same size is released as is.
                .acquire_many(nbytes.try_into().unwrap_or(u32::MAX))
                .await?;
            permit.forget();
        }

        Ok(())
    }

    /// Release nbytes resource to this global limit.
    pub fn release(&self, nbytes: impl TryInto<u32>) {
        if self.is_exit {
            self.inner
                .inner
                // If a single size exceeds 4 GB, just release 4 GB.
                .add_permits(nbytes.try_into().unwrap_or(u32::MAX) as usize);
        }
    }
}

/// Defines that a struct has a known length in bytes.
#[allow(clippy::len_without_is_empty)]
pub trait Lengthed {
    fn len(&self) -> usize;
}

// Impl this trait for Item types used in AsyncIterators.
impl Lengthed for Vec<u8> {
    fn len(&self) -> usize {
        self.len()
    }
}

impl<T: Sized> Lengthed for (T, Vec<u8>) {
    fn len(&self) -> usize {
        std::mem::size_of::<T>() + self.1.len()
    }
}

#[cfg(test)]
mod test {
    use crate::buffered_async_iterator::test_utils::*;
    use async_trait::async_trait;
    use std::{mem::size_of, time::Duration};

    use crate::{AsyncIterator, BufferedAsyncIterator, GlobalMemoryLimit, MemoryLimit};

    struct AsyncIterWithDelay<It: AsyncIterator<()>> {
        data: It,
        delay_in_msec: u64,
    }

    #[async_trait]
    impl<T: AsyncIterator<()>> AsyncIterator<()> for AsyncIterWithDelay<T>
    where
        T::Item: Into<u64>,
    {
        type Item = u64;

        async fn next(&mut self) -> Result<Option<Self::Item>, ()> {
            tokio::time::sleep(Duration::from_millis(self.delay_in_msec)).await;
            self.data.next().await.map(|ret| ret.map(|op| op.into()))
        }
    }

    #[tokio::test]
    #[ignore = "will deadlock"]
    async fn demonstrate_deadlock() {
        // 5 * 8 bytes memory
        let limit = MemoryLimit::new(5 * size_of::<u64>());

        let input = (0u64..100).into_iter().collect();

        // input -> | src_iter (fast) -> buffered_iter_1 -> compute_task (slow) -> buffered_iter_2 | -> output

        let src_iter = AsyncIterWithDelay {
            data: VecIterator::new(0, input),
            delay_in_msec: 1,
        };

        let buffered_iter_1 = BufferedAsyncIterator::new(
            src_iter,
            Some(100),
            Some(GlobalMemoryLimit::entry_and_exit(&limit)),
        );

        let compute_task = AsyncIterWithDelay {
            data: buffered_iter_1,
            delay_in_msec: 10,
        };

        let mut buffered_iter_2 = BufferedAsyncIterator::new(
            compute_task,
            Some(100),
            Some(GlobalMemoryLimit::entry_and_exit(&limit)),
        );

        loop {
            let output = buffered_iter_2.next().await.unwrap();
            if output.is_none() {
                break;
            }
        }
    }

    // Use the same setting as above but the correct usage of GlobalMemoryLimit
    #[tokio::test]
    async fn demonstrate_usage() {
        // 5 * 8 bytes memory
        let limit = MemoryLimit::new(5 * size_of::<u64>());

        let input = (0u64..100).into_iter().collect();

        // input -> | src_iter (fast) -> buffered_iter_1 -> compute_task (slow) -> buffered_iter_2 | -> output

        let src_iter = AsyncIterWithDelay {
            data: VecIterator::new(0, input),
            delay_in_msec: 1,
        };

        let buffered_iter_1 = BufferedAsyncIterator::new(
            src_iter,
            Some(100),
            Some(GlobalMemoryLimit::entry_only(&limit)),
        );

        let compute_task = AsyncIterWithDelay {
            data: buffered_iter_1,
            delay_in_msec: 10,
        };

        let mut buffered_iter_2 = BufferedAsyncIterator::new(
            compute_task,
            Some(100),
            Some(GlobalMemoryLimit::exit_only(&limit)),
        );

        loop {
            let output = buffered_iter_2.next().await.unwrap();
            if output.is_none() {
                break;
            }
        }

        assert_eq!(limit.inner.available_permits(), 5 * size_of::<u64>());
    }
}
