use crate::constants::GIT_MAX_PACKET_SIZE;
use std::io::Write;

use crate::errors::Result;
use crate::stream::git_stream_frame::{GitFrame, GitStatus};
use tracing::{debug, error};

/// Writes GitFrame objects to a stream
pub struct GitStreamWriter<W: Write> {
    writer: W,
}

impl<W: Write> GitStreamWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    // Helper function to write out a data packet in chunks according to the
    // git spec.
    fn write_git_data_packet(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let chunks = data.chunks(GIT_MAX_PACKET_SIZE);
        let n_chunks = chunks.len();

        for chunk in chunks {
            self.write_frame(chunk)?;
        }
        debug!(
            "XET: Wrote {:?} bytes of data in {:?} chunks.",
            data.len(),
            n_chunks
        );
        Ok(data.len())
    }

    // Utility to write out the data plus the size header
    fn write_frame(&mut self, b: &[u8]) -> std::io::Result<usize> {
        debug!("Writing frame of len {}", b.len());
        self.writer
            .write_all(format!("{:04x}", b.len() + 4).as_bytes())?;
        self.writer.write_all(b)?;
        Ok(b.len() + 4)
    }

    /// Writes out a GitFrame object to the stream according to the git long
    /// running process spec.
    ///
    /// # Arguments
    ///
    /// * `frame` - the GitFrame object to write.
    pub fn write_value(&mut self, frame: &GitFrame) -> std::io::Result<usize> {
        match frame {
            GitFrame::Filter(val) => self.write_frame(format!("{val}\n").as_bytes()),
            GitFrame::Status(val) => self.write_frame(format!("{val}\n").as_bytes()),
            GitFrame::Capability(val) => self.write_frame(format!("{val}\n").as_bytes()),
            GitFrame::Flush => {
                self.writer.write_all("0000".as_bytes())?;
                let _ = self.writer.flush();
                Ok(4)
            }
            GitFrame::Command(val) => self.write_frame(format!("{val}\n").as_bytes()),
            GitFrame::PathInfo(val) => self.write_frame(format!("pathname={val}\n").as_bytes()),
            GitFrame::Data(val) => self.write_git_data_packet(val),
            GitFrame::Version(val) => self.write_frame(format!("version={val}\n").as_bytes()),
            GitFrame::CanDelay(val) => self.write_frame(format!("can-delay={val}\n").as_bytes()),
        }
    }

    /// Writes out a processed data frame to the stream according to the
    /// git long running process spec.
    ///
    /// # Arguments
    ///
    /// * `data` - the data to write out to the stream
    pub fn start_data_transmission(&mut self) -> Result<()> {
        debug!("Starting data transmit");
        self.write_value(&GitFrame::Status(GitStatus::Success))?;
        self.write_value(&GitFrame::Flush)?;
        Ok(())
    }

    /// Writes out a processed data frame to the stream according to the
    /// git long running process spec.
    ///
    /// # Arguments
    ///
    /// * `data` - the data to write out to the stream
    pub fn write_data_frame(&mut self, data: Vec<u8>) -> Result<()> {
        debug!("Writing data frame of len {}", data.len());
        self.write_value(&GitFrame::Data(data))?;
        Ok(())
    }
    pub fn end_data_transmission(&mut self) -> Result<()> {
        debug!("ending data transmit");
        self.write_value(&GitFrame::Flush)?;
        self.write_value(&GitFrame::Flush)?;
        Ok(())
    }

    /// A utility function for writing out error to the stream according to the
    /// git long running process spec.
    pub fn write_error_frame(&mut self) -> Result<()> {
        error!("Writing error frame");
        self.write_value(&GitFrame::Status(GitStatus::Error))?;
        self.write_value(&GitFrame::Flush)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::stream::git_stream_frame::{
        GitCapability, GitCommand, GitFilterType, GitFrame, GitStatus,
    };
    use mockstream::MockStream;

    #[test]
    fn test_write_git_flush() {
        let mut s = MockStream::new();

        let mut writer = GitStreamWriter { writer: &mut s };

        let res = writer.write_value(&GitFrame::Flush);

        let output = s.pop_bytes_written();

        assert!(res.is_ok());
        assert_eq!("0000".as_bytes(), output);
    }

    #[test]
    fn test_write_value() {
        let mut s: Vec<u8> = Vec::new();
        let mut writer = GitStreamWriter { writer: &mut s };

        let frame = GitFrame::Flush;
        let res = writer.write_value(&frame);
        assert!(res.is_ok());
        assert_eq!(s, b"0000");

        let mut s: Vec<u8> = Vec::new();
        let mut writer = GitStreamWriter { writer: &mut s };

        let frame = GitFrame::Filter(GitFilterType::Server);
        let res = writer.write_value(&frame);
        assert!(res.is_ok());
        assert_eq!(s, b"0016git-filter-server\n");

        let mut s: Vec<u8> = Vec::new();
        let mut writer = GitStreamWriter { writer: &mut s };

        let frame = GitFrame::Version(4);
        let res = writer.write_value(&frame);
        assert!(res.is_ok());
        assert_eq!(s, b"000eversion=4\n");

        let mut s: Vec<u8> = Vec::new();
        let mut writer = GitStreamWriter { writer: &mut s };

        let frame = GitFrame::Status(GitStatus::Success);
        let res = writer.write_value(&frame);
        assert!(res.is_ok());
        assert_eq!(s, b"0013status=success\n");

        let mut s: Vec<u8> = Vec::new();
        let mut writer = GitStreamWriter { writer: &mut s };

        let frame = GitFrame::Status(GitStatus::Error);
        let res = writer.write_value(&frame);
        assert!(res.is_ok());
        assert_eq!(s, b"0011status=error\n");

        let mut s: Vec<u8> = Vec::new();
        let mut writer = GitStreamWriter { writer: &mut s };

        let frame = GitFrame::Capability(GitCapability::Clean);
        let res = writer.write_value(&frame);
        assert!(res.is_ok());
        assert_eq!(s, b"0015capability=clean\n");

        let mut s: Vec<u8> = Vec::new();
        let mut writer = GitStreamWriter { writer: &mut s };

        let frame = GitFrame::Capability(GitCapability::Delay);
        let res = writer.write_value(&frame);
        assert!(res.is_ok());
        assert_eq!(s, b"0015capability=delay\n");

        let mut s: Vec<u8> = Vec::new();
        let mut writer = GitStreamWriter { writer: &mut s };

        let frame = GitFrame::Capability(GitCapability::Smudge);
        let res = writer.write_value(&frame);
        assert!(res.is_ok());
        assert_eq!(s, b"0016capability=smudge\n");

        let mut s: Vec<u8> = Vec::new();
        let mut writer = GitStreamWriter { writer: &mut s };

        let frame = GitFrame::PathInfo("/foo/bar".to_string());
        let res = writer.write_value(&frame);
        assert!(res.is_ok());
        assert_eq!(s, b"0016pathname=/foo/bar\n");

        let mut s: Vec<u8> = Vec::new();
        let mut writer = GitStreamWriter { writer: &mut s };

        let frame = GitFrame::Command(GitCommand::Clean);
        let res = writer.write_value(&frame);
        assert!(res.is_ok());
        assert_eq!(s, b"0012command=clean\n");

        let mut s: Vec<u8> = Vec::new();
        let mut writer = GitStreamWriter { writer: &mut s };

        let frame = GitFrame::Command(GitCommand::Smudge);
        let res = writer.write_value(&frame);
        assert!(res.is_ok());
        assert_eq!(s, b"0013command=smudge\n");

        let mut s: Vec<u8> = Vec::new();
        let mut writer = GitStreamWriter { writer: &mut s };

        let frame = GitFrame::Data("testdatafoo".as_bytes().to_vec());
        let res = writer.write_value(&frame);
        assert!(res.is_ok());
        assert_eq!(s, b"000ftestdatafoo");
    }

    // testing arbitrary sized buffers
    fn check_data_write_with_buffer_size(buffer_size: usize, frames: usize) {
        let bytes: Vec<u8> = (0..buffer_size)
            .map(|i| (i % (GIT_MAX_PACKET_SIZE)) as u8)
            .collect();

        let mut s: Vec<u8> = Vec::new();
        let mut writer = GitStreamWriter { writer: &mut s };

        let frame = GitFrame::Data(bytes);
        let res = writer.write_value(&frame);
        assert!(res.is_ok());

        // size + 4 x frames (headers)
        assert_eq!(s.len(), buffer_size + 4 * frames);
        for i in 0..frames {
            let start = i * (GIT_MAX_PACKET_SIZE + 4);
            // 0xff + 4 = 0x103
            assert_eq!(
                &s[start..start + 4],
                format!(
                    "{:04x}",
                    std::cmp::min(GIT_MAX_PACKET_SIZE, buffer_size - (i * GIT_MAX_PACKET_SIZE)) + 4
                )
                .as_bytes()
            );

            for j in 0..GIT_MAX_PACKET_SIZE {
                if start + 4 + j >= buffer_size + 4 * frames {
                    break;
                }
                assert_eq!(s[start + 4 + j], j as u8);
            }
        }
    }

    #[test]
    fn test_write_large_buffer() {
        // boundary checking around multiples of 255
        check_data_write_with_buffer_size(GIT_MAX_PACKET_SIZE - 1, 1);
        check_data_write_with_buffer_size(GIT_MAX_PACKET_SIZE, 1);
        check_data_write_with_buffer_size(GIT_MAX_PACKET_SIZE + 1, 2);
        check_data_write_with_buffer_size(2 * GIT_MAX_PACKET_SIZE - 1, 2);
        check_data_write_with_buffer_size(4 * GIT_MAX_PACKET_SIZE - 2, 4);
        check_data_write_with_buffer_size(4 * GIT_MAX_PACKET_SIZE - 1, 4);
        check_data_write_with_buffer_size(4 * GIT_MAX_PACKET_SIZE + 1, 5);
    }
}
