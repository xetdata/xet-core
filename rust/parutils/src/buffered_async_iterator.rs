use std::collections::VecDeque;
use std::ops::DerefMut;

use async_trait::async_trait;
// use deadqueue::limited::Queue;
use tokio::sync::oneshot;

use crate::async_iterator::*;

enum BufferItem<T: Send + Sync + 'static, E: Send + Sync + 'static> {
    Value(T),
    Completed,
    Error(E),
}

pub struct BufferedBlockingIterator<It: AsyncIterator<E> + 'static, E: Send + Sync + 'static> {
    initial_data: Vec<It::Item>,
    src_iter: It,
}

impl<It: AsyncIterator<E> + 'static, E: Send + Sync + 'static> BufferedBlockingIterator<It, E> {
    pub fn new(src_iter: It, buffer_max_size: Option<usize>) -> Self {
        Self::new_with_starting_data(vec![], src_iter, buffer_max_size)
    }

    pub fn new_with_starting_data(
        initial_data: Vec<It::Item>,
        src_iter: It,
        buffer_max_size: Option<usize>,
    ) -> Self {
        // Set up the send and receive channels.

        Self {
            initial_data,
            src_iter,
        }
    }
}

#[async_trait]
impl<It: AsyncIterator<E>, E: Send + Sync + 'static> AsyncIterator<E>
    for BufferedBlockingIterator<It, E>
{
    type Item = It::Item;

    /// The traditional next method; returns None if everything is done.
    async fn next(&mut self) -> Result<Option<Self::Item>, E> {
        if !self.initial_data.is_empty() {
            return Ok(Some(self.initial_data.remove(0)));
        }
        self.src_iter.next().await
    }
}

#[async_trait]
impl<It: AsyncIterator<E>, E: Send + Sync + 'static> BatchedAsyncIterator<E>
    for BufferedBlockingIterator<It, E>
{
    async fn next_batch(&mut self, max_num: Option<usize>) -> Result<Vec<Self::Item>, E> {
        let mut v = Vec::with_capacity(max_num.unwrap_or(1));
        let mut count = 0;
        while let Some(i) = self.next().await? {
            v.push(i);
            count += 1;
            if let Some(max) = max_num {
                if count >= max {
                    break;
                }
            }
        }
        Ok(v)
    }

    /// If known, returns the number of items remaining.
    fn items_remaining(&self) -> Option<usize> {
        None
    }
}

// /// Now, create a buffered stream.  The 'static lifetime specifiers essentially require the objects to be owned
// /// objects that do not contain any references.
// pub struct BufferedAsyncIterator<It: AsyncIterator<E> + 'static, E: Send + Sync + 'static> {
//     // Use dead simple queue here as it provides exactly the functionality we need for the batching part.
//     data_queue: Arc<Queue<BufferItem<It::Item, E>>>,
//
//     // True if all data is in the queue above and nothing more is coming.
//     completion_flag: Arc<AtomicBool>,
//
//     // Do we need to clean things up?
//     background_handle: Option<JoinHandle<()>>,
//
//     // Just to make the lookup tables correct
//     _marker: PhantomData<(It, E)>,
// }
//
// impl<It: AsyncIterator<E> + 'static, E: Send + Sync + 'static> BufferedAsyncIterator<It, E> {
//     /// Create an instance of the iterator with all the values present up front.
//     /// This will just iterate through the remaining elements and then stop.
//     pub fn new_complete(data: Vec<It::Item>) -> Self {
//         let data_queue = Arc::new(Queue::new(data.len() + 1));
//         for v in data {
//             let _ = data_queue.try_push(BufferItem::Value(v));
//         }
//         // End of data is signaled by a None coming through.
//         let _ = data_queue.try_push(BufferItem::Completed);
//
//         Self {
//             data_queue,
//             completion_flag: Arc::new(AtomicBool::new(true)),
//             background_handle: None,
//             _marker: Default::default(),
//         }
//     }
//
//     /// Create an instance of the iterator that wraps a given input stream.  If given,
//     /// the buffer_max_size parameter dictates how many elements to store in the local
//     /// buffer at a given time.
//     pub fn new(src_iter: It, buffer_max_size: Option<usize>) -> Self {
//         Self::new_with_starting_data(vec![], src_iter, buffer_max_size)
//     }
//
//     /// Create an instance of the iterator initialized with a buffer of items.  Items will
//     /// be pulled off this initial queue first, then pulled from the given input stream.
//     ///
//     /// If given, the buffer_max_size parameter dictates how many elements to store in the local
//     /// buffer at a given time.
//     ///
//     /// If given, stream_return is a tokio oneshot channel that is called by the reading task as soon as the iterator
//     /// is completed.  At that point, all items have been read into the local buffer.
//     pub fn new_with_starting_data(
//         initial_data: Vec<It::Item>,
//         src_iter: It,
//         buffer_max_size: Option<usize>,
//     ) -> Self {
//         Self::new_impl(initial_data, src_iter, buffer_max_size, None)
//     }
//
//     /// Like new_with_starting_data, but also creates a tokio oneshot channel to return the src_iter object as
//     /// soon as it returns None and all items are in the buffer.
//     ///
//     /// This is intended to enable the following pattern:
//     ///
//     /// let (b_iter, iterator_return) = BufferedAsyncIterator::new_with_iterator_return(data, src_iter, None);
//     ///
//     /// let jh = tokio::spawn(async move {
//     ///    // Send off the iterator to a background thread.
//     ///    process_data(b_iter)
//     /// });
//     ///
//     /// // Get back the src_iter object as soon as it returns None, even though process_data
//     /// // is still working through the buffer.
//     ///
//     /// let src_iter = iterator_return.await;
//     ///
//     pub fn new_with_iterator_return(
//         initial_data: Vec<It::Item>,
//         src_iter: It,
//         buffer_max_size: Option<usize>,
//     ) -> (Self, oneshot::Receiver<It>) {
//         let (stream_sendback, iterator_return) = oneshot::channel::<It>();
//
//         let s = Self::new_impl(
//             initial_data,
//             src_iter,
//             buffer_max_size,
//             Some(stream_sendback),
//         );
//
//         (s, iterator_return)
//     }
//
//     fn new_impl(
//         initial_data: Vec<It::Item>,
//         src_iter: It,
//         buffer_max_size: Option<usize>,
//         stream_sendback: Option<oneshot::Sender<It>>,
//     ) -> Self {
//         // Set up the send and receive channels.
//
//         let buffer_max_size = buffer_max_size.unwrap_or(2048).max(initial_data.len());
//         let data_queue = Arc::new(Queue::new(buffer_max_size + 1));
//         for v in initial_data {
//             let _ = data_queue.try_push(BufferItem::Value(v)); // Only fails when out of capacity; never a problem here.
//         }
//
//         let completion_flag = Arc::new(AtomicBool::new(false));
//
//         let background_handle = Some(Self::start_retrieval_task(
//             src_iter,
//             data_queue.clone(),
//             stream_sendback,
//             completion_flag.clone(),
//         ));
//
//         Self {
//             data_queue,
//             completion_flag,
//             background_handle,
//             _marker: Default::default(),
//         }
//     }
//
//     /// True if all the data is stored locally and no more will be pulled
//     /// from the input stream.  If this is true, then the next call
//     /// to next_batch() could return everything.
//     pub fn all_items_in_buffer(&self) -> bool {
//         self.completion_flag
//             .load(std::sync::atomic::Ordering::Acquire)
//     }
//
//     fn start_retrieval_task(
//         mut src_iter: It,
//         data_queue: Arc<Queue<BufferItem<It::Item, E>>>,
//         mut stream_sendback: Option<oneshot::Sender<It>>,
//         completion_flag: Arc<AtomicBool>,
//     ) -> JoinHandle<()> {
//         tokio::spawn(async move {
//             loop {
//                 let incoming = match src_iter.next().await {
//                     Ok(incoming) => incoming,
//                     Err(e) => {
//                         // Signal the stream is closed, then send back the error.
//                         data_queue.push(BufferItem::Error(e)).await;
//                         completion_flag.store(true, std::sync::atomic::Ordering::SeqCst);
//                         break;
//                     }
//                 };
//
//                 let Some(v) = incoming else {
//                     data_queue.push(BufferItem::Completed).await;
//                     completion_flag.store(true, std::sync::atomic::Ordering::SeqCst);
//                     break;
//                 };
//                 data_queue.push(BufferItem::Value(v)).await;
//             }
//
//             if let Some(sendback) = stream_sendback.take() {
//                 if sendback.send(src_iter).is_err() {
//                     info!("Error sending iterator back on tokio stream; receiver closed.");
//                 }
//             }
//         })
//     }
//
//     /// Forces the background task to run to completion.  Used in testing. to ensure that the
//     /// completion flag has been properly set.  Should not be needed in most cases.
//     pub async fn cleanup_background_task(&mut self) -> Result<(), JoinError> {
//         if let Some(jh) = self.background_handle.take() {
//             jh.await?;
//         }
//         Ok(())
//     }
// }
//
// impl<It: AsyncIterator<E>, E: Send + Sync + 'static> Drop for BufferedAsyncIterator<It, E> {
//     fn drop(&mut self) {
//         if let Some(jh) = self.background_handle.take() {
//             jh.abort();
//         }
//     }
// }

// #[async_trait]
// impl<It: AsyncIterator<E>, E: Send + Sync + 'static> AsyncIterator<E>
// for BufferedAsyncIterator<It, E>
// {
//     type Item = It::Item;
//
//     /// The traditional next method; returns None if everything is done.
//     async fn next(&mut self) -> Result<Option<Self::Item>, E> {
//         match self.data_queue.pop().await {
//             BufferItem::Value(v) => Ok(Some(v)),
//             BufferItem::Completed => Ok(None),
//             BufferItem::Error(e) => Err(e),
//         }
//     }
// }

// #[async_trait]
// impl<It: AsyncIterator<E>, E: Send + Sync + 'static> BatchedAsyncIterator<E>
// for BufferedAsyncIterator<It, E>
// {
//     async fn next_batch(&mut self, max_num: Option<usize>) -> Result<Vec<Self::Item>, E> {
//         let n_ready = self.data_queue.len();
//
//         // If there are None ready currently, do we wait for more or be done with it?
//         if n_ready == 0 && self.all_items_in_buffer() {
//             return Ok(Vec::new());
//         }
//
//         // Now, there will be at least one coming down the line, so block on that.
//         let n_ready = n_ready.max(1);
//
//         // We have a batch ready, so pull them all in.
//         let n_retrieve = max_num.unwrap_or(n_ready).min(n_ready);
//         let mut ret = Vec::with_capacity(n_retrieve);
//
//         for _ in 0..n_retrieve {
//             match self.data_queue.pop().await {
//                 BufferItem::Value(v) => ret.push(v),
//                 BufferItem::Completed => break,
//                 BufferItem::Error(e) => return Err(e),
//             };
//         }
//         Ok(ret)
//     }
//
//     /// If known, returns the number of items remaining.
//     fn items_remaining(&self) -> Option<usize> {
//         if self.all_items_in_buffer() {
//             let n = self.data_queue.len();
//             // Subtract one for the BufferItem::Completed item coming down the queue.
//             Some(n.saturating_sub(1))
//         } else {
//             None
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use async_trait::async_trait;
    use more_asserts::*;

    use super::*;

    struct VecIterator {
        data: VecDeque<u64>,
        id: u64,
    }

    impl VecIterator {
        fn new(id: u64, data: Vec<u64>) -> Self {
            VecIterator {
                data: VecDeque::from(data),
                id,
            }
        }
    }

    #[async_trait]
    impl AsyncIterator<()> for VecIterator {
        type Item = u64;

        async fn next(&mut self) -> Result<Option<Self::Item>, ()> {
            Ok(self.data.pop_front())
        }
    }

    async fn make_batch_iterator(
        id: u64,
        mut data: Vec<u64>,
        starting_method: usize,
        buffer_size: Option<usize>,
    ) -> (
        BufferedBlockingIterator<VecIterator, ()>,
        Option<oneshot::Receiver<VecIterator>>,
    ) {
        // if starting_method == 0 {
        let src_iter = VecIterator::new(id, data.clone());

        (BufferedBlockingIterator::new(src_iter, buffer_size), None)
        // } else if starting_method == 1 {
        //     // Use the fully encapsulated version.
        //
        //     (BufferedBlockingIterator::new_complete(data), None)
        // } else {
        //     let iter_data = data.split_off(data.len() / 2);
        //
        //     let src_iter = VecIterator::new(id, iter_data);
        //
        //     let (iter, it_ret) =
        //         BufferedBlockingIterator::new_with_iterator_return(data, src_iter, buffer_size);
        //     (iter, Some(it_ret))
        // }
    }

    async fn run_test_with_vec(data: Vec<u64>) {
        let mut id_n = 100000;
        for starting_method in [0, 1, 2] {
            for buffer_size in [Some(1), Some(7), Some(5000)] {
                // First test the strait up next method.
                id_n += 1;
                let (mut batch_iter, mut iterator_return) =
                    make_batch_iterator(id_n, data.clone(), starting_method, buffer_size).await;

                for i in 0..data.len() {
                    if let Some(n_r) = batch_iter.items_remaining() {
                        assert_eq!(n_r, data.len() - i);
                    }

                    let v = batch_iter.next().await.unwrap();

                    assert!(v.is_some());
                    assert_eq!(v.unwrap(), data[i]);
                }
                assert!(batch_iter.next().await.unwrap().is_none());

                if let Some(it_ret) = iterator_return.take() {
                    let v = it_ret.await.unwrap();
                    assert_eq!(v.id, id_n);
                }

                // Test the next_batch method
                for batch_size in [Some(1), Some(3), None] {
                    id_n += 1;
                    let (mut batch_iter, mut iterator_return) =
                        make_batch_iterator(id_n, data.clone(), starting_method, buffer_size).await;

                    let mut out_data = Vec::with_capacity(data.len());
                    loop {
                        if let Some(n_r) = batch_iter.items_remaining() {
                            assert_eq!(n_r, data.len() - out_data.len());
                        }

                        let v = batch_iter.next_batch(batch_size).await.unwrap();

                        if v.is_empty() {
                            // batch_iter.cleanup_background_task().await.unwrap();
                            // assert_eq!(batch_iter.items_remaining(), Some(0));
                            break;
                        }

                        if let Some(bs) = &batch_size {
                            assert_le!(v.len(), bs);
                        }

                        out_data.extend(v);
                    }

                    assert!(batch_iter.next_batch(None).await.unwrap().is_empty());
                    assert_eq!(out_data, data);

                    if let Some(it_ret) = iterator_return.take() {
                        let v = it_ret.await.unwrap();
                        assert_eq!(v.id, id_n);
                    }
                }
            }
        }
    }

    #[tokio::test()]
    async fn test_corner_cases() {
        run_test_with_vec(vec![]).await;
        run_test_with_vec(vec![1]).await;
    }

    #[tokio::test()]
    async fn test_buffer() {
        run_test_with_vec(vec![1, 2, 3, 4]).await;
    }

    #[tokio::test()]
    async fn test_large_random() {
        run_test_with_vec(Vec::from_iter(0..2000)).await;
    }
}
