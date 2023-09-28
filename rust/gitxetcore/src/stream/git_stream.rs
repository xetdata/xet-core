use std::{
    io::{Read, Write},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use crate::{
    data_processing::PointerFileTranslator,
    errors::{GitXetRepoError, Result},
    stream::file_reader::FileChannelReader,
    stream::git_stream_frame::{GitCapability, GitCommand, GitFilterType, GitFrame, GitStatus},
    stream::stream_reader::{GitStreamReadIterator, StreamStatus},
    stream::stream_writer::GitStreamWriter,
};
use fallible_iterator::FallibleIterator;
use lazy_static::lazy_static;
use pointer_file::PointerFile;
use progress_reporting::DataProgressReporter;
use regex::Regex;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::watch;
use tokio::sync::RwLock;
use tracing::{debug, error, info, info_span, warn, Span};
use tracing_futures::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// The GIT Stream version we support.
const EXPECTED_GIT_VERSION: u32 = 2;

/// The channel limit for the read side of the handler
/// The git packet size is limited to 64K-ish.
/// so this limits the channel volume to 256MB. This is also
/// the maximum file size we can "pass-through".
const GIT_READ_MPSC_CHANNEL_SIZE: usize = 4096;
/// The channel limit for the write side of the handler
/// The write handler may write messages up to 16MB.
/// so this limits the per-channel volume to about 256 MB
const GIT_WRITE_MPSC_CHANNEL_SIZE: usize = 16;
const GIT_MAX_SMUDGE_DELAY_SLOTS: usize = 20480;
/// The number ot active bytes we are waiting on. This number can be increased
/// in higher network performance regimes; at a cost of
/// increased memory utilization.
const GIT_MAX_SMUDGE_DELAY_BYTES: usize = 256 * 1024 * 1024;

// The numbers above are tuned to be pretty symmetric;
// If I have 1 large file; the channel consumption is no more than ~ 256MB.
// If I have a ton of tiny files; we are never smudging more than 256MB.
// Total memory usage in practice should be upper bounded by
// 256MB + O(# active files)

// A utility for cleaning up our verification code.
macro_rules! check_and_error {
    ($condition:expr, $msg:expr) => {{
        if !$condition {
            error!("{} {}", $msg, $condition);
            return Err(GitXetRepoError::StreamParseError(format!(
                "{} {}",
                $msg, $condition
            )));
        }
    }};
}

// A macro to log and return an error when an EOF is found
// unexpectedly.
macro_rules! unexpected_eof {
    () => {{
        error!("Unexpected EOF in stream");
        return Err(GitXetRepoError::StreamParseError(
            "Unexpected EOF in stream".to_string(),
        ));
    }};
}

// Utility used to clean up verifying the handshake
fn is_client_filter(frame: Option<GitFrame>) -> bool {
    matches!(frame, Some(GitFrame::Filter(GitFilterType::Client)))
}

struct HandlerChannels {
    write: Sender<Option<Vec<u8>>>,
    read: Receiver<Result<Vec<u8>>>,
    ready: Option<watch::Receiver<bool>>,
    /// Number of bytes we are smudging. Note that is *entirely*
    /// only for bookkeeping by the smudge queueing system
    /// to ensure that to the total number of bytes we are waiting for
    /// is bounded. It should not be set by anyone except by
    /// `check_for_smudge_tasks_to_do()`
    smudging_volume: usize,
}

struct QueueEntry {
    path: String,
    frames: Vec<GitFrame>,
    file_len: usize,
}

impl HandlerChannels {
    fn new(
        write: Sender<Option<Vec<u8>>>,
        read: Receiver<Result<Vec<u8>>>,
        ready: Option<watch::Receiver<bool>>,
    ) -> HandlerChannels {
        HandlerChannels {
            write,
            read,
            ready,
            smudging_volume: 0,
        }
    }
}

/// The git stream processing struct.
pub struct GitStreamInterface<R: Read, W: Write> {
    reader: GitStreamReadIterator<R>,
    writer: GitStreamWriter<W>,
    pub repo: Arc<RwLock<PointerFileTranslator>>,

    /// A map from path -> handlers. A list of all the active clean/smudge handlers.
    handler_channels: HashMap<String, HandlerChannels>,
    /// Tasks to be smudged
    queued_smudge_tasks: VecDeque<QueueEntry>,
    /// Total number of bytes currently being smudged in handlers
    total_active_smudging_volume: usize,

    /// Specific flags that might need to be handled later.
    lfs_pointers_present_on_smudge: Arc<AtomicBool>,

    /// Progress indicator.  This is printed only on the clean commands.  
    /// The first element indicates whether it's been active, and the second is the progress indicator class.
    clean_progress: Option<Arc<DataProgressReporter>>,

    /// Progress indicator.  This is printed only on the smudge commands.  
    /// The first element indicates whether it's been active, and the second is the progress indicator class.
    smudge_progress: Option<Arc<DataProgressReporter>>,

    /// Tests can stick their own read/write handlers here
    /// which will be used instead of the the regular clean/smudge channels.
    #[cfg(test)]
    test_channels: HashMap<String, HandlerChannels>,
}

/// Returns true if the &[u8] buffer parses as a pointer file
fn test_packet_is_pointer_file(data: &[u8], path: &str) -> Option<usize> {
    let file_str: &str = match std::str::from_utf8(data) {
        Ok(v) => v,
        Err(_) => {
            return None;
        }
    };
    let ptr_file = PointerFile::init_from_string(file_str, path);
    if ptr_file.is_valid() {
        Some(ptr_file.filesize() as usize)
    } else {
        None
    }
}

/// Sends a git frame through a channel.
/// Returns true if the frame is an EOF (flush)
async fn send_git_frame_through_channel(
    channel: &Sender<Option<Vec<u8>>>,
    frame: GitFrame,
) -> Result<bool> {
    let mut done = false;
    let chan_write_result = match frame {
        GitFrame::Data(data) => channel.send(Some(data)).await,
        GitFrame::Flush => {
            done = true;
            channel.send(None).await
        }
        _ => {
            return Err(GitXetRepoError::Other(format!(
                "Unexpected git packet {frame:?}"
            )));
        }
    };
    chan_write_result.map_err(|_| {
        GitXetRepoError::Other("Unable to send bytes for clean/smudge as channel has closed".into())
    })?;
    Ok(done)
}

impl<R: Read + Send + 'static, W: Write> GitStreamInterface<R, W> {
    /// Returns a GitStreamInterface.
    ///
    /// # Arguments
    ///
    /// * `io_reader` - the input stream
    /// * `io_writer` - the output stream
    pub fn new_with_progress(io_reader: R, io_writer: W, repo: PointerFileTranslator) -> Self {
        Self {
            reader: GitStreamReadIterator::new(io_reader),
            writer: GitStreamWriter::new(io_writer),
            repo: Arc::new(RwLock::new(repo)),
            handler_channels: HashMap::new(),
            queued_smudge_tasks: VecDeque::new(),
            total_active_smudging_volume: 0,
            lfs_pointers_present_on_smudge: Arc::new(AtomicBool::new(false)),
            clean_progress: Some(DataProgressReporter::new_inactive(
                "Xet: Deduplicating data blocks",
                None,
                None,
            )),
            smudge_progress: Some(DataProgressReporter::new_inactive(
                "Xet: Retrieving data blocks",
                None,
                None,
            )),
            #[cfg(test)]
            test_channels: HashMap::new(),
        }
    }

    pub fn new(io_reader: R, io_writer: W, repo: PointerFileTranslator) -> Self {
        Self {
            reader: GitStreamReadIterator::new(io_reader),
            writer: GitStreamWriter::new(io_writer),
            repo: Arc::new(RwLock::new(repo)),
            handler_channels: HashMap::new(),
            queued_smudge_tasks: VecDeque::new(),
            total_active_smudging_volume: 0,
            lfs_pointers_present_on_smudge: Arc::new(AtomicBool::new(false)),
            clean_progress: None,
            smudge_progress: None,

            #[cfg(test)]
            test_channels: HashMap::new(),
        }
    }

    /// Performs the git handshake process according to the long running filter
    /// process spec.
    ///
    /// Returns Ok on success and Err on failure.
    pub async fn establish_git_handshake(&mut self) -> Result<()> {
        // first check the for git-filter-client
        check_and_error!(
            is_client_filter(self.reader.next()?),
            "Incorrect filter type on git handshake"
        );

        // check whether our expected version is among the versions
        // According to the spec, we should expect multiple versions and the
        // one we want will be listed:
        // https://git-scm.com/docs/gitattributes#_long_running_filter_process
        let mut expected_version = false;
        loop {
            let frame = self.reader.next()?;
            match frame {
                Some(GitFrame::Version(version)) => {
                    if version == EXPECTED_GIT_VERSION {
                        expected_version = true;
                    }
                }
                None => unexpected_eof!(),
                _ => break,
            }
        }
        check_and_error!(
            expected_version,
            format!("Required version={EXPECTED_GIT_VERSION} not found")
        );

        // write the handshake response
        self.writer
            .write_value(&GitFrame::Filter(GitFilterType::Server))?;
        self.writer.write_value(&GitFrame::Version(2))?;
        self.writer.write_value(&GitFrame::Flush)?;

        // check for our supported capabilities
        let mut caps = (false, false, false);
        loop {
            let frame = self.reader.next()?;
            match frame {
                Some(GitFrame::Capability(GitCapability::Clean)) => caps.0 = true,
                Some(GitFrame::Capability(GitCapability::Smudge)) => caps.1 = true,
                Some(GitFrame::Capability(GitCapability::Delay)) => caps.2 = true,
                None => unexpected_eof!(),
                _ => break,
            }
        }
        check_and_error!(
            caps.0 && caps.1 && caps.2,
            "Clean, smudge, and delay must all be supported"
        );

        // write out the capabilities we support
        self.writer
            .write_value(&GitFrame::Capability(GitCapability::Clean))?;
        self.writer
            .write_value(&GitFrame::Capability(GitCapability::Smudge))?;
        self.writer
            .write_value(&GitFrame::Capability(GitCapability::Delay))?;
        self.writer.write_value(&GitFrame::Flush)?;

        Ok(())
    }

    /// Process the stream and smudge or clean the data. This is the primary mover of
    /// the streaming protocol. Returns Ok on success and Err on failure.
    ///
    /// # Arguments
    ///
    /// * `repo` - the MerkleDB repo object
    pub async fn run_git_event_loop(&mut self) -> Result<()> {
        loop {
            match self.read_git_input().await {
                Ok(StreamStatus::Eof) => {
                    debug!("Shutting down reader task.");
                    break;
                }
                Ok(_) => {
                    continue;
                }
                Err(e) => {
                    error!("ERROR: Error in polling process: {:?}", e);
                    return Err(e);
                }
            }
        }
        // we call finalize_cleaning whether or not cleaning actually
        // happened. This is safe to do.
        self.repo.write().await.finalize_cleaning().await?;

        // Print final messages of progress indicators.
        for pi in [&self.clean_progress, &self.smudge_progress]
            .into_iter()
            .flatten()
        {
            pi.finalize();
        }

        if self
            .lfs_pointers_present_on_smudge
            .as_ref()
            .load(Ordering::Relaxed)
        {
            eprintln!("git-lfs pointers detected in current checkout.  Run \n\n  git lfs fetch && git lfs checkout\n\nto convert these files to git-xet objects, then commit them to the repository.");
        }

        Ok(())
    }

    /// Return true if there is at least 1 handler which is ready
    fn has_ready_handlers(&self) -> bool {
        for (_, chans) in self.handler_channels.iter() {
            // has a ready channel and it has been flagged as ready
            if let Some(ready) = &chans.ready {
                if *ready.borrow() {
                    return true;
                }
            } else {
                // do not have a ready channel; so ready by default
                return true;
            }
        }
        false
    }

    #[inline]
    fn check_packet_is_lfs_pointer_file(&self, data: &[u8], path: &str) -> bool {
        // Check whether the given path was detected as an lfs pointer file.

        if data.len() > 512 {
            // LFS pointer files should be less than .5k
            return false;
        }

        if let Ok(beginning_stub) = std::str::from_utf8(&data[..data.len().min(256)]) {
            lazy_static! {
                static ref GIT_LFS_RE: Regex = Regex::new("^version http.*git-lfs.*").unwrap();
            }

            if GIT_LFS_RE.is_match(beginning_stub) {
                info!("File {} detected as lfs pointer.", path);

                self.lfs_pointers_present_on_smudge
                    .store(true, Ordering::Relaxed);

                return true;
            }
        }

        false
    }

    /// Builds the response to the list_available_blobs command
    async fn respond_to_list_available_blobs(&mut self) -> Result<StreamStatus> {
        info!("List Blobs");
        // we need at least 1 blob to be ready
        tokio::task::yield_now().await;
        if !self.has_ready_handlers() {
            // nothing is ready. wait for the first channel in the list
            for (_, chans) in self.handler_channels.iter_mut() {
                if let Some(ready) = &mut chans.ready {
                    if *ready.borrow() {
                        break;
                    } else {
                        let _ = ready.changed().await;
                        break;
                    }
                }
            }
        }
        // loop through all the handlers and write out if something
        // is ready
        let mut nready = 0;
        for (k, chans) in self.handler_channels.iter() {
            if let Some(ready) = &chans.ready {
                if *ready.borrow() {
                    self.writer.write_value(&GitFrame::PathInfo(k.clone()))?;
                    nready += 1;
                }
            } else {
                self.writer.write_value(&GitFrame::PathInfo(k.clone()))?;
                nready += 1;
            }
        }
        info!(
            "{} / {} channels ready.",
            nready,
            self.handler_channels.len(),
        );
        self.writer.write_value(&GitFrame::Flush)?;
        self.writer
            .write_value(&GitFrame::Status(GitStatus::Success))?;
        self.writer.write_value(&GitFrame::Flush)?;
        Ok(StreamStatus::Normal(0))
    }

    /// Returns true if the smudge was delayed and false otherwise.
    async fn respond_to_smudge_command(&mut self, delayable: u32, path: &str) -> Result<bool> {
        //read 1 frame and check if it is a pointer file
        let first_frame = match self.reader.next_expect_data()? {
            Some(f) => f,
            None => {
                unexpected_eof!()
            }
        };

        let is_pointer_file = match &first_frame {
            GitFrame::Data(data) => {
                if self.check_packet_is_lfs_pointer_file(data, path) {
                    None
                } else {
                    test_packet_is_pointer_file(data, path)
                }
            }
            _ => None,
        };
        // check if we can delay it.
        if delayable > 0 && is_pointer_file.is_some() {
            // if we can, we will queue the tasks and return immediately.
            info!("Delaying {:?}", &path);
            // we finish reading all the frames
            // stopping on a Flush frame.
            // Note that the first frame may be a Flush frame
            // (technically that is impossible since this must be
            // a pointer file and hence the first_frame must contain
            // data), but we will keep this loop defensive.
            let mut frames: Vec<GitFrame> = vec![first_frame];
            while !matches!(frames.last().unwrap(), GitFrame::Flush) {
                match self.reader.next_expect_data()? {
                    Some(f) => frames.push(f),
                    None => {
                        unexpected_eof!()
                    }
                };
            }
            let file_len = is_pointer_file.unwrap_or_default();
            self.queued_smudge_tasks.push_back(QueueEntry {
                path: path.to_string(),
                frames,
                file_len,
            });
            self.writer
                .write_value(&GitFrame::Status(GitStatus::Delayed))?;
            self.writer.write_value(&GitFrame::Flush)?;
            self.check_for_smudge_tasks_to_do().await?;
            Ok(true)
        } else {
            // if we can't delay it, we will do the normal route
            //
            // If the channel does not exist, we are smudging a new file.
            // create the channels and feed the file contents to the channel
            // We check the test channel first. Then make the regular
            // smudge channel. Note that we need to pass on the first frame
            // we have read.
            #[cfg(test)]
            self.initialize_with_test_channels(path);
            self.initialize_channels(GitCommand::Smudge, path).await;

            self.git_read_file_contents(path, Some(first_frame)).await?;
            Ok(false)
        }
    }

    /// Reads from the git packet stream expecting to receive a flush packet
    fn git_read_expect_flush(&mut self) -> Result<()> {
        let frame = self.reader.next()?;
        match frame {
            Some(GitFrame::Flush) => {}
            _ => {
                return Err(GitXetRepoError::StreamParseError(format!(
                    "Expecting flush, but got {frame:?}"
                )));
            }
        }
        Ok(())
    }
    /// Initializes the current channels by copying from the test channel
    #[cfg(test)]
    fn initialize_with_test_channels(&mut self, path: &str) {
        if let Some(v) = self.test_channels.remove(path) {
            self.handler_channels.insert(path.to_string(), v);
        }
    }

    /// Handles 1 git command / response pair.
    /// A git command is either:
    ///   - clean
    ///   - smudge
    ///   - list_available_blobs
    async fn read_git_input(&mut self) -> Result<StreamStatus> {
        // pull the command out of the stream
        // including the options we understand until we hit a flush
        let mut command: Option<GitCommand> = None;
        let mut delayable: u32 = 0;
        let mut path: String = String::new();
        loop {
            let frame = self.reader.next()?;
            match frame {
                Some(GitFrame::Flush) => break,
                Some(GitFrame::Command(value)) => {
                    command = Some(value);
                }
                Some(GitFrame::PathInfo(value)) => {
                    path = value;
                }
                Some(GitFrame::CanDelay(value)) => {
                    delayable = value;
                }
                None => {
                    return Ok(StreamStatus::Eof);
                }
                _ => continue,
            }
        }

        // there must be a command
        let route = command
            .ok_or_else(|| GitXetRepoError::StreamParseError("command not received".to_string()))?;

        let mut span = Span::current();
        if span.is_disabled() {
            span = info_span!("gitxet", "command" = "filter", "path" = path);
        }

        if let GitCommand::ListAvailableBlobs = route {
            return self
                .respond_to_list_available_blobs()
                .instrument(span)
                .await;
        }

        // If we reach here, it is either a clean or a smudge
        debug!("XET: Reading in {}", path);
        warn!("{route:?}");

        if !self.handler_channels.contains_key(&path) {
            match route {
                GitCommand::Smudge => {
                    if self
                        .respond_to_smudge_command(delayable, &path)
                        .instrument(span)
                        .await?
                    {
                        return Ok(StreamStatus::Normal(0));
                    }
                }
                GitCommand::Clean => {
                    // If the channel does not exist, we are cleaning a new file.
                    // create the channels and feed the file contents to the channel
                    // We check the test channel first. Then make the regular
                    // cleaning channel
                    #[cfg(test)]
                    self.initialize_with_test_channels(&path);
                    self.initialize_channels(route, &path)
                        .instrument(span)
                        .await;

                    self.git_read_file_contents(&path, None).await?;
                }
                _ => {
                    panic!("Impossible unhandled command {route:?}");
                }
            }
        } else {
            // if the channel already exists this is a resume
            // and we should read out a flush packet
            self.git_read_expect_flush()?;
        }

        // response to clean/smudge
        self.writer.start_data_transmission()?;
        if let Some(chans) = &mut self.handler_channels.get_mut(&path) {
            while let Some(processed) = chans.read.recv().await {
                match processed {
                    Err(e) => {
                        error!(
                            "XET: Error processing {:?}, writing error back to stream",
                            &path
                        );
                        self.writer.write_error_frame()?;
                        return Err(e);
                    }
                    Ok(data) => {
                        self.writer.write_data_frame(data)?;
                    }
                }
            }
            self.writer.end_data_transmission()?;
            self.total_active_smudging_volume -= chans.smudging_volume;
        } else {
            panic!("Unable to find {path:?} in handler channel list");
        }
        self.handler_channels.remove(&path);
        self.check_for_smudge_tasks_to_do().await?;

        Ok(StreamStatus::Normal(0))
    }

    /// Creates the clean/smudge channel for a given path
    /// and for a given route direction.
    ///
    /// The smudging_volume argument is *entirely*
    /// only for bookkeeping by the smudge queueing system
    /// to ensure that to the total number of bytes we are waiting for
    /// is bounded. It should not be set by anyone except by
    /// `check_for_smudge_tasks_to_do()`
    /// Anyone else using this should just set this to 0.
    async fn initialize_channels(&mut self, route: GitCommand, path: &str) {
        // check if the channel already exists
        if self.handler_channels.contains_key(path) {
            return;
        }
        let path = path.to_string();

        match route {
            GitCommand::Clean => {
                info!(
                    "XET: Recieved request for clean for {:?}; sending to clean.",
                    &path
                );
                let (tx_file, rx_file) = channel::<Option<Vec<u8>>>(GIT_READ_MPSC_CHANNEL_SIZE);
                let (tx_res, rx_res) = channel::<Result<Vec<u8>>>(GIT_WRITE_MPSC_CHANNEL_SIZE);

                let cur_span = Span::current();
                let ctx = cur_span.context();
                let repo = self.repo.clone();

                self.handler_channels
                    .insert(path.clone(), HandlerChannels::new(tx_file, rx_res, None));
                let progress_indicator = self.clean_progress.clone();

                tokio::spawn(async move {
                    let wlock = repo.write().await;
                    let task_span = info_span!("clean_file", ?path);
                    task_span.set_parent(ctx);

                    let reader = FileChannelReader::new(rx_file);

                    let res = wlock
                        .clean_file_and_report_progress(
                            &PathBuf::from(path),
                            reader,
                            &progress_indicator,
                        )
                        .instrument(task_span)
                        .await;

                    if res.is_err() {
                        error!("Clean error {:?}", &res);
                    }

                    if tx_res.send(res).await.is_err() {
                        error!("Unable to send cleaned result as channel has closed");
                    }
                });
            }
            GitCommand::Smudge => {
                info!(
                    "XET: Recieved request for smudge for {:?}; sending to smudge.",
                    &path
                );
                let (tx_file, rx_file) = channel::<Option<Vec<u8>>>(GIT_READ_MPSC_CHANNEL_SIZE);
                let (tx_res, rx_res) = channel::<Result<Vec<u8>>>(GIT_WRITE_MPSC_CHANNEL_SIZE);
                let (tx_ready, rx_ready) = watch::channel::<bool>(false);

                let cur_span = Span::current();
                let ctx = cur_span.context();
                let repo = self.repo.clone();
                self.handler_channels.insert(
                    path.clone(),
                    HandlerChannels::new(tx_file, rx_res, Some(rx_ready)),
                );
                let progress_indicator = self.smudge_progress.clone();

                tokio::spawn(async move {
                    let rlock = repo.read().await;
                    let task_span = info_span!("smudge_file", ?path);
                    task_span.set_parent(ctx);
                    rlock
                        .smudge_file_to_mpsc(
                            &PathBuf::from(path),
                            FileChannelReader::new(rx_file),
                            &tx_res,
                            &Some(tx_ready),
                            &progress_indicator,
                        )
                        .instrument(task_span)
                        .await;
                });
            }
            _ => {
                panic!("Unexpected command");
            }
        }
    }

    /// Read a file content stream for a given path from git
    /// forwarding the stream contents to to the handler channel for the stream
    /// for cleaning/smudging. An optional first_frame is provided.
    async fn git_read_file_contents(
        &mut self,
        path: &str,
        first_frame: Option<GitFrame>,
    ) -> Result<()> {
        if let Some(chans) = self.handler_channels.get(path) {
            // handle the first frame if provide
            if let Some(fframe) = first_frame {
                let is_flush = matches!(fframe, GitFrame::Flush);
                if send_git_frame_through_channel(&chans.write, fframe)
                    .await
                    .is_err()
                    || is_flush
                {
                    return Ok(());
                }
            }
            while let Some(frame) = self.reader.next_expect_data()? {
                let done = send_git_frame_through_channel(&chans.write, frame).await?;
                if done {
                    break;
                }
            }
        }
        Ok(())
    }

    /// check if number of active channels is below GIT_MAX_SMUDGE_DELAY_SLOTS
    /// and if there are tasks in the queue to start up
    async fn check_for_smudge_tasks_to_do(&mut self) -> Result<()> {
        while !self.queued_smudge_tasks.is_empty()
            && self.handler_channels.len() < GIT_MAX_SMUDGE_DELAY_SLOTS
            && self.total_active_smudging_volume < GIT_MAX_SMUDGE_DELAY_BYTES
        {
            // we just checked that it is not empty above
            // pull out a task
            let task = self.queued_smudge_tasks.pop_front().unwrap();
            // create the channels and kick start it
            #[cfg(test)]
            self.initialize_with_test_channels(&task.path);
            self.initialize_channels(GitCommand::Smudge, &task.path)
                .await;
            self.total_active_smudging_volume += task.file_len;
            // feed the frame sequence to the channel
            if let Some(chans) = self.handler_channels.get_mut(&task.path) {
                chans.smudging_volume = task.file_len;
                for frame in task.frames {
                    let done = send_git_frame_through_channel(&chans.write, frame).await?;
                    if done {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mdb_shard::shard_version::ShardVersion;
    use mockstream::MockStream;
    use tempfile::TempDir;

    #[derive(Clone)]
    struct SafeStream {
        stream: Arc<std::sync::Mutex<MockStream>>,
    }

    impl SafeStream {
        pub fn new() -> Self {
            Self {
                stream: Arc::new(std::sync::Mutex::new(MockStream::new())),
            }
        }

        fn pop_bytes_written(&mut self) -> Vec<u8> {
            self.stream.lock().unwrap().pop_bytes_written()
        }
    }

    impl Write for SafeStream {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.stream.lock().unwrap().write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.stream.lock().unwrap().flush()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_establish_git_handshake_success() {
        for mdb_version in [ShardVersion::V1, ShardVersion::V2] {
            let mut reader = MockStream::new();
            let mut writer = SafeStream::new();
            let stagedir = TempDir::new().unwrap();

            reader.push_bytes_to_read(b"0016git-filter-client\n000eversion=2\n");
            reader.push_bytes_to_read(b"0000");
            reader.push_bytes_to_read(b"0015capability=clean\n");
            reader.push_bytes_to_read(b"0016capability=smudge\n");
            reader.push_bytes_to_read(b"0015capability=delay\n");
            reader.push_bytes_to_read(b"0000");

            let mut interface = GitStreamInterface::new(
                reader,
                writer.clone(),
                PointerFileTranslator::new_temporary(stagedir.path(), mdb_version)
                    .await
                    .unwrap(),
            );

            let res = interface.establish_git_handshake().await;

            eprintln!("{:?}", res);

            assert!(res.is_ok());

            let value = writer.pop_bytes_written();

            assert_eq!(
                r#"0016git-filter-server
000eversion=2
00000015capability=clean
0016capability=smudge
0015capability=delay
0000"#
                    .as_bytes(),
                value
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_establish_git_handshake_multiple_versions() {
        for mdb_version in [ShardVersion::V1, ShardVersion::V2] {
            let mut reader = MockStream::new();
            let mut writer = SafeStream::new();
            let stagedir = TempDir::new().unwrap();

            reader.push_bytes_to_read(b"0016git-filter-client\n");
            reader.push_bytes_to_read(b"000eversion=2\n");
            reader.push_bytes_to_read(b"000fversion=42\n");
            reader.push_bytes_to_read(b"0000");
            reader.push_bytes_to_read(b"0015capability=clean\n");
            reader.push_bytes_to_read(b"0016capability=smudge\n");
            reader.push_bytes_to_read(b"0015capability=delay\n");
            reader.push_bytes_to_read(b"0000");

            let mut interface = GitStreamInterface::new(
                reader,
                writer.clone(),
                PointerFileTranslator::new_temporary(stagedir.path(), mdb_version)
                    .await
                    .unwrap(),
            );

            let res = interface.establish_git_handshake().await;

            eprintln!("{:?}", res);

            assert!(res.is_ok());

            let value = writer.pop_bytes_written();
            eprintln!("{}", std::str::from_utf8(&value).unwrap());
            assert_eq!(
                br#"0016git-filter-server
000eversion=2
00000015capability=clean
0016capability=smudge
0015capability=delay
0000"#
                    .to_vec(),
                value
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_git_handshake_wrong_versions() {
        for mdb_version in [ShardVersion::V1, ShardVersion::V2] {
            let mut reader = MockStream::new();
            let writer = MockStream::new();
            let stagedir = TempDir::new().unwrap();

            reader.push_bytes_to_read(b"0016git-filter-client\n");
            reader.push_bytes_to_read(b"000eversion=1\n");
            reader.push_bytes_to_read(b"000fversion=42\n");
            reader.push_bytes_to_read(b"0000");

            let mut interface = GitStreamInterface::new(
                reader,
                writer,
                PointerFileTranslator::new_temporary(stagedir.path(), mdb_version)
                    .await
                    .unwrap(),
            );

            let res = interface.establish_git_handshake().await;

            assert!(res.is_err());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_git_handshake_no_versions() {
        for mdb_version in [ShardVersion::V1, ShardVersion::V2] {
            let mut reader = MockStream::new();
            let writer = MockStream::new();
            let stagedir = TempDir::new().unwrap();

            reader.push_bytes_to_read(b"0016git-filter-client\n");
            reader.push_bytes_to_read(b"0015capability=clean\n");
            reader.push_bytes_to_read(b"0016capability=smudge\n");
            reader.push_bytes_to_read(b"0015capability=delay\n");
            reader.push_bytes_to_read(b"0000");

            let mut interface = GitStreamInterface::new(
                reader,
                writer,
                PointerFileTranslator::new_temporary(stagedir.path(), mdb_version)
                    .await
                    .unwrap(),
            );

            let res = interface.establish_git_handshake().await;

            assert!(res.is_err());
        }
    }

    async fn verify_read_input(
        path: String,
        bytes: &'static [u8],
        mdb_version: ShardVersion,
    ) -> Option<Vec<u8>> {
        let _reader = MockStream::new();
        let stagedir = TempDir::new().unwrap();

        let (tx, mut rx) = channel::<Option<Vec<u8>>>(1);
        let (tx_bytes, rx_bytes) = channel::<Result<Vec<u8>>>(1);

        tokio::spawn(async move {
            let mut stream = MockStream::new();
            let writer = MockStream::new();

            stream.push_bytes_to_read(bytes);

            let mut interface = GitStreamInterface::new(
                stream,
                writer,
                PointerFileTranslator::new_temporary(stagedir.path(), mdb_version)
                    .await
                    .unwrap(),
            );
            // we fake the existance of a clean/smudge channel by
            // setting our own handler
            interface
                .test_channels
                .insert(path, HandlerChannels::new(tx, rx_bytes, None));
            let res = interface.read_git_input().await;

            assert!(res.is_ok());
        });

        // here we are basically acting as the clean/smudge handler.
        // we receive the ff, and we reply with a completion
        let mut ret: Option<Vec<u8>> = None;
        while let Some(Some(data)) = rx.recv().await {
            eprintln!("Picked up a data");
            ret = Some(data);
        }
        tx_bytes.send(Ok(Vec::new())).await.unwrap();
        drop(tx_bytes);

        ret
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_git_input_smudge() {
        for mdb_version in [ShardVersion::V1, ShardVersion::V2] {
            let bytes = br#"0013command=smudge
0016pathname=/foo/bar
00000012file contents
0000"#;

            match verify_read_input("/foo/bar".to_string(), bytes, mdb_version).await {
                Some(ff) => {
                    assert_eq!(ff, b"file contents\n".to_vec());
                }
                _ => panic!("smudge failed"),
            }

            let bytes = br#"0012command=clean
0016pathname=/foo/bar
00000012file contents
0000"#;

            match verify_read_input("/foo/bar".to_string(), bytes, mdb_version).await {
                Some(ff) => {
                    assert_eq!(ff, b"file contents\n".to_vec());
                }
                _ => panic!("clean failed"),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_git_input_smudge_empty() {
        for mdb_version in [ShardVersion::V1, ShardVersion::V2] {
            // we should not read past the two flushes (0000).
            // It is an error if we do as that means we will
            // start reading into the next command.
            // if fails the GARBAGE gets read the parser will explode
            let bytes = br#"0013command=smudge
0016pathname=/foo/bar
00000000GARBAGE
"#;
            if let Some(ff) = verify_read_input("/foo/bar".to_string(), bytes, mdb_version).await {
                panic!("smudge incorrect {:?}", ff);
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_wait_and_read_git_packet_multiple_frames() {
        for mdb_version in [ShardVersion::V1, ShardVersion::V2] {
            let (tx, mut rx) = channel::<Option<Vec<u8>>>(1);
            let (tx_bytes, rx_bytes) = channel::<Result<Vec<u8>>>(1);

            tokio::spawn(async move {
                let mut stream = MockStream::new();
                let writer = MockStream::new();
                let stagedir = TempDir::new().unwrap();

                stream.push_bytes_to_read(b"0012command=clean\n");
                stream.push_bytes_to_read(b"0016pathname=/bar/baz\n");
                stream.push_bytes_to_read(b"0000");
                stream.push_bytes_to_read(b"0014file contents 1\n");
                stream.push_bytes_to_read(b"0014file contents 2\n");
                stream.push_bytes_to_read(b"0000");

                let mut interface = GitStreamInterface::new(
                    stream,
                    writer,
                    PointerFileTranslator::new_temporary(stagedir.path(), mdb_version)
                        .await
                        .unwrap(),
                );
                // we fake the existance of a clean/smudge channel by
                // setting our own handler
                interface.test_channels.insert(
                    "/bar/baz".to_string(),
                    HandlerChannels::new(tx, rx_bytes, None),
                );
                let res = interface.read_git_input().await;

                eprintln!("{:?}", res);
                assert!(res.is_ok());
            });

            // here we are basically acting as the clean/smudge handler.
            // we receive the ff, and we reply with a completion
            let mut fragment_index = 0;
            while let Some(ff) = rx.recv().await {
                match fragment_index {
                    0 => {
                        assert!(ff.is_some());
                        assert_eq!(ff.unwrap(), b"file contents 1\n");
                        fragment_index += 1;
                    }
                    1 => {
                        assert!(ff.is_some());
                        assert_eq!(ff.unwrap(), b"file contents 2\n");
                        fragment_index += 1;
                    }
                    2 => {
                        assert!(ff.is_none());
                        break;
                    }
                    _ => panic!(),
                }
            }
            tx_bytes.send(Ok(Vec::new())).await.unwrap();
            drop(tx_bytes);
        }
    }

    // TODO: In order to unit test process_file and run_git_event_loop, we need to
    // restructure the DataProcessingManager class definition. Currently, it does not
    // support automock and breaking out clean_file and smudge_file into a separate
    // trait is not working due to a huge cascade of Sync + Send requirements throughout
    // the project.
    // #[tokio::test]
    // async fn test_process_file() {}
    // #[tokio::test]
    // async fn run_git_event_loop() {}
}
