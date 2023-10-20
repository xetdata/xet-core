use fallible_iterator::FallibleIterator;
use std::io::{ErrorKind, Read};
use tracing::trace;

const GIT_STREAM_FRAME_HEADER_SIZE: usize = 4;

use crate::{
    errors::{GitXetRepoError, Result},
    stream::git_stream_frame::GitFrame,
};

/// Represents the different success cases for
/// reading a frame from the stream.
///
/// Note: this is a candidate for a future
/// refactor since the GitFrame enum contains
/// equivalent information.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum StreamStatus {
    Normal(usize),
    Flush,
    Eof,
}

/// An iterator implementation for the git stream input.
/// Note: This class makes use of the fallible_iterator crate
/// because we needed support for returning Results from
/// the next iterator.
pub struct GitStreamReadIterator<R: Read> {
    reader: R,
}

impl<R: Read> GitStreamReadIterator<R> {
    /// Returns a new iterator
    ///
    /// # Arguments
    ///
    /// * `reader` - the input read object (stream)
    pub fn new(reader: R) -> Self {
        GitStreamReadIterator { reader }
    }

    // Utility that reads the frame size from the first 4 bytes.
    // Eg: "000a" -> 10.
    fn read_frame_size(&mut self) -> Result<StreamStatus> {
        let mut len_hex = [0u8; GIT_STREAM_FRAME_HEADER_SIZE];

        if let Err(e) = self.reader.read_exact(&mut len_hex) {
            return if e.kind() == ErrorKind::UnexpectedEof {
                Ok(StreamStatus::Eof)
            } else {
                Err(GitXetRepoError::IOError(e))
            };
        }

        let byte_string = String::from_utf8_lossy(&len_hex);

        Ok(StreamStatus::Normal(usize::from_str_radix(
            &byte_string,
            16,
        )?))
    }

    /// Get the next parsed frame from the stream.
    ///
    /// Returns an error if the processing couldn't complete,
    /// otherwise, it'll return a frame.
    pub fn next_expect_data(&mut self) -> Result<Option<GitFrame>> {
        // get the frame size from the stream
        let result = self.read_frame_size()?;

        // Handle all the edge cases for frame sizes
        let data_len = match result {
            StreamStatus::Eof => return Ok(None),
            StreamStatus::Normal(0) => return Ok(Some(GitFrame::Flush)),
            StreamStatus::Normal(x) if x < GIT_STREAM_FRAME_HEADER_SIZE => {
                return Err(GitXetRepoError::StreamParseError(format!(
                    "The input packet is corrupt - packet length is {x:?}. Must be greater than 4."
                )));
            }
            StreamStatus::Normal(x) => x - GIT_STREAM_FRAME_HEADER_SIZE,
            _ => {
                return Err(GitXetRepoError::StreamParseError(
                    "Unexpected header parsing error".to_string(),
                ))
            }
        };

        // read the data frame
        trace!("XET: Reading in expected {} bytes.", data_len);

        let mut buffer = vec![0; data_len];

        self.reader
            .read_exact(&mut buffer[..])
            .map_err(GitXetRepoError::IOError)?;

        Ok(Some(GitFrame::parse_data_or_flush_frame(buffer)?))
    }
}

impl<R: Read> FallibleIterator for GitStreamReadIterator<R> {
    type Item = GitFrame;
    type Error = GitXetRepoError;

    /// Get the next parsed frame from the stream.
    ///
    /// Returns an error if the processing couldn't complete,
    /// otherwise, it'll return a frame.
    fn next(&mut self) -> Result<Option<Self::Item>> {
        // get the frame size from the stream
        let result = self.read_frame_size()?;

        // Handle all the edge cases for frame sizes
        let data_len = match result {
            StreamStatus::Eof => return Ok(None),
            StreamStatus::Normal(0) => return Ok(Some(GitFrame::Flush)),
            StreamStatus::Normal(x) if x < GIT_STREAM_FRAME_HEADER_SIZE => {
                return Err(GitXetRepoError::StreamParseError(format!(
                    "The input packet is corrupt - packet length is {x:?}. Must be greater than 4."
                )));
            }
            StreamStatus::Normal(x) => x - GIT_STREAM_FRAME_HEADER_SIZE,
            _ => {
                return Err(GitXetRepoError::StreamParseError(
                    "Unexpected header parsing error".to_string(),
                ))
            }
        };

        // read the data frame
        trace!("XET: Reading in expected {} bytes.", data_len);

        let mut buffer = vec![0; data_len];

        self.reader
            .read_exact(&mut buffer[..])
            .map_err(GitXetRepoError::IOError)?;

        Ok(Some(GitFrame::parse_frame(buffer)?))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        errors::Result,
        stream::git_stream_frame::{GitCapability, GitCommand, GitStatus},
    };
    use mockstream::MockStream;

    macro_rules! assert_err {
        ($expression:expr, $($pattern:tt)+) => {
            match $expression {
                $($pattern)+ => (),
                ref e => panic!("expected `{}` but got `{:?}`", stringify!($($pattern)+), e),
            }
        }
    }

    fn git_read_frame_with_data(data: &str) -> Result<Option<GitFrame>> {
        let mut s = MockStream::new();
        s.push_bytes_to_read(data.as_bytes());

        let mut reader = GitStreamReadIterator::new(&mut s);

        reader.next()
    }

    fn read_frame_util(data: &str) -> Result<StreamStatus> {
        let mut s = MockStream::new();
        s.push_bytes_to_read(data.as_bytes());
        let mut reader = GitStreamReadIterator::new(&mut s);
        reader.read_frame_size()
    }

    #[test]
    fn test_read_frame() {
        assert_eq!(read_frame_util("0000"), Ok(StreamStatus::Normal(0)));
        assert_eq!(read_frame_util("0001"), Ok(StreamStatus::Normal(1)));
        assert_eq!(read_frame_util("ffff"), Ok(StreamStatus::Normal(65535)));
        assert_eq!(read_frame_util("fffe"), Ok(StreamStatus::Normal(65534)));
        assert_eq!(read_frame_util("ff"), Ok(StreamStatus::Eof));
        assert_eq!(read_frame_util("00"), Ok(StreamStatus::Eof));
        assert_eq!(read_frame_util("a"), Ok(StreamStatus::Eof));
        assert_eq!(read_frame_util("000"), Ok(StreamStatus::Eof));
    }

    #[test]
    fn test_read_git_packet_eof() -> Result<()> {
        let res = git_read_frame_with_data("")?;
        assert!(res.is_none());
        Ok(())
    }

    #[test]
    fn test_bad_file_sizes() -> Result<()> {
        let res = git_read_frame_with_data("0001ab");
        assert!(res.is_err());
        assert_err!(res, Err(GitXetRepoError::StreamParseError(_)));
        let res = git_read_frame_with_data("0002ab");
        assert!(res.is_err());
        assert_err!(res, Err(GitXetRepoError::StreamParseError(_)));
        let res = git_read_frame_with_data("0003ab");
        assert!(res.is_err());
        assert_err!(res, Err(GitXetRepoError::StreamParseError(_)));

        Ok(())
    }

    #[test]
    fn test_read_git_frame() {
        let res = git_read_frame_with_data("0006ab");

        assert!(res.is_ok());
        assert_eq!(Ok(Some(GitFrame::Data(b"ab".to_vec()))), res);
    }

    #[test]
    fn test_flush_operation() {
        let res = git_read_frame_with_data("0000");
        assert!(res.is_ok());
        assert_eq!(Ok(Some(GitFrame::Flush)), res);
    }

    #[test]
    fn test_clean_caps() {
        let res = git_read_frame_with_data("0015capability=clean\n");
        assert!(res.is_ok());
        assert_eq!(Ok(Some(GitFrame::Capability(GitCapability::Clean))), res);
    }

    #[test]
    fn test_clean_command() {
        let res = git_read_frame_with_data("0012command=clean\n");
        assert!(res.is_ok());
        assert_eq!(Ok(Some(GitFrame::Command(GitCommand::Clean))), res);
    }

    #[test]
    fn test_version() {
        let res = git_read_frame_with_data("000eversion=2\n");
        assert!(res.is_ok());
        assert_eq!(Ok(Some(GitFrame::Version(2))), res);
    }

    #[test]
    fn test_status_error() {
        let res = git_read_frame_with_data("0011status=error\n");
        assert!(res.is_ok());
        assert_eq!(Ok(Some(GitFrame::Status(GitStatus::Error))), res);
    }

    #[test]
    fn test_pathname() {
        let res = git_read_frame_with_data("0016pathname=/foo/bar\n");
        assert!(res.is_ok());
        assert_eq!(Ok(Some(GitFrame::PathInfo("/foo/bar".to_string()))), res);
    }

    #[test]
    fn test_eof_means_none() {
        let res = git_read_frame_with_data("");
        assert!(res.is_ok());
        assert_eq!(Ok(None), res);
    }
}
