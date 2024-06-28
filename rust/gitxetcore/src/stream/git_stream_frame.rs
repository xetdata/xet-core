use atoi::atoi;

use tracing::error;

use crate::errors::{GitXetRepoError, Result};

// Writes out errors and returns an exception from a buffer object.
#[macro_export]
macro_rules! write_buff_error {
    ($msg:expr, $buffer:expr) => {{
        let s = std::str::from_utf8($buffer)?;
        error!("{} {}", $msg, s);
        Err(GitXetRepoError::StreamParseError(format!("{} {}", $msg, s)))
    }};
}

// Utility to compare a buffer against another buffer
fn compare(a: &[u8], b: &[u8]) -> bool {
    if a.len().cmp(&b.len()) != std::cmp::Ordering::Equal {
        return false;
    }

    // if the lengths were equal, starts_with performs comparison
    starts_with(a, b)
}

// Utility to compare a buffer's start against another buffer
fn starts_with(a: &[u8], b: &[u8]) -> bool {
    for (ai, bi) in a.iter().zip(b.iter()) {
        match ai.cmp(bi) {
            std::cmp::Ordering::Equal => continue,
            _ => return false,
        }
    }
    true
}

// Utility to return the value location for a key=value string.
fn get_value_for_key(bytes: &[u8], search: &[u8]) -> Option<usize> {
    if bytes.len() > search.len() {
        return None;
    }

    let key_len = bytes.len();
    if compare(bytes, &search[..key_len]) {
        return Some(key_len);
    }
    None
}

/// The trait implementation for parsing a buffer into
/// the implementing type.
pub trait GitStreamParse {
    fn parse_buffer(buffer: &[u8]) -> Result<Self>
    where
        Self: Sized + PartialEq;
}

/// Represents the headers of the input/output
/// handshake frames.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GitFilterType {
    /// git-filter-client
    Client,
    /// git-filter-server
    Server,
}

impl std::fmt::Display for GitFilterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GitFilterType::Client => write!(f, "git-filter-client"),
            GitFilterType::Server => write!(f, "git-filter-server"),
        }
    }
}

impl GitStreamParse for GitFilterType {
    fn parse_buffer(buffer: &[u8]) -> Result<Self> {
        if starts_with(b"git-filter-client", buffer) {
            return Ok(GitFilterType::Client);
        }
        if starts_with(b"git-filter-server", buffer) {
            return Ok(GitFilterType::Server);
        }

        write_buff_error!("Unsupported git filter type: ", buffer)
    }
}

/// Represents the four supported status types from the spec.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GitStatus {
    /// status=success representation
    Success,
    /// status=error representation
    Error,
    /// status=abort representation
    Abort,
    /// status=delayed representation
    Delayed,
}

impl std::fmt::Display for GitStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GitStatus::Success => write!(f, "status=success"),
            GitStatus::Error => write!(f, "status=error"),
            GitStatus::Abort => write!(f, "status=abort"),
            GitStatus::Delayed => write!(f, "status=delayed"),
        }
    }
}

impl GitStreamParse for GitStatus {
    fn parse_buffer(buffer: &[u8]) -> Result<Self> {
        if starts_with(b"status=success", buffer) {
            return Ok(GitStatus::Success);
        }
        if starts_with(b"status=abort", buffer) {
            return Ok(GitStatus::Abort);
        }
        if starts_with(b"status=error", buffer) {
            return Ok(GitStatus::Error);
        }
        if starts_with(b"status=delayed", buffer) {
            return Ok(GitStatus::Delayed);
        }
        write_buff_error!("Unsupported git status type: ", buffer)
    }
}

/// Represents the capability options in the protocol
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GitCapability {
    /// capability=clean
    Clean,
    /// capability=smudge
    Smudge,
    /// capability=delay
    Delay,
}

impl std::fmt::Display for GitCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GitCapability::Clean => write!(f, "capability=clean"),
            GitCapability::Smudge => write!(f, "capability=smudge"),
            GitCapability::Delay => write!(f, "capability=delay"),
        }
    }
}

impl GitStreamParse for GitCapability {
    fn parse_buffer(buffer: &[u8]) -> Result<Self> {
        if starts_with(br"capability=clean", buffer) {
            return Ok(GitCapability::Clean);
        }
        if starts_with(br"capability=smudge", buffer) {
            return Ok(GitCapability::Smudge);
        }
        if starts_with(br"capability=delay", buffer) {
            return Ok(GitCapability::Delay);
        }
        write_buff_error!("Unsupported git capability type: ", buffer)
    }
}

/// Represents the different command supported by the stream
/// NOTE: According the git long running process spec alludes
/// to other commands, but there isn't an easily accessible list
/// of what those are. We'll have to add support for those later.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GitCommand {
    /// command=clean
    Clean,
    /// command=smudge
    Smudge,
    /// command=list_available_blobs
    ListAvailableBlobs,
}

impl std::fmt::Display for GitCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GitCommand::Clean => write!(f, "command=clean"),
            GitCommand::Smudge => write!(f, "command=smudge"),
            GitCommand::ListAvailableBlobs => write!(f, "command=list_available_blobs"),
        }
    }
}

impl GitStreamParse for GitCommand {
    fn parse_buffer(buffer: &[u8]) -> Result<Self> {
        if starts_with(br"command=clean", buffer) {
            return Ok(GitCommand::Clean);
        }
        if starts_with(br"command=smudge", buffer) {
            return Ok(GitCommand::Smudge);
        }
        if starts_with(br"command=list_available_blobs", buffer) {
            return Ok(GitCommand::ListAvailableBlobs);
        }
        write_buff_error!("Unsupported git capability type: ", buffer)
    }
}

/// Represents a variable git frame type. Each frame is delineated by
/// a 4 byte header indicating the frame size.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GitFrame {
    /// <empty frame>
    Flush,
    // handshake frames
    /// git-filter-client | git-filter-server
    Filter(GitFilterType),
    /// version=<value>
    Version(u32),
    /// capability=<smudge | clean | delay>
    Capability(GitCapability),
    // responses
    /// status=<error | abort | delay | success>
    Status(GitStatus),
    // filter data
    /// command=<smudge | clean>
    Command(GitCommand),
    /// pathname=<path to file>
    PathInfo(String),
    /// can-delay=1
    CanDelay(u32),
    /// <data>
    Data(Vec<u8>),
}

impl GitFrame {
    pub fn parse_version(buffer: &[u8]) -> Result<u32> {
        if let Some(first) = get_value_for_key(b"version=", buffer) {
            return match atoi::<u32>(&buffer[first..]) {
                Some(v) => Ok(v),
                None => Err(GitXetRepoError::StreamParseError(
                    "Could not parse version string".to_string(),
                )),
            };
        }
        write_buff_error!("Unable to parse version from ", buffer)
    }

    pub fn parse_can_delay(buffer: &[u8]) -> Result<u32> {
        if let Some(first) = get_value_for_key(b"can-delay=", buffer) {
            return match atoi::<u32>(&buffer[first..]) {
                Some(v) => Ok(v),
                None => Err(GitXetRepoError::StreamParseError(
                    "Could not parse can-delay string".to_string(),
                )),
            };
        }
        write_buff_error!("Unable to parse can-delay from ", buffer)
    }

    /// Parses the data into a GitFrame object. Returns an Err on
    /// failure.
    ///
    /// # Arguments
    ///
    /// * `buffer` - the input data to parse
    pub fn parse_frame(buffer: Vec<u8>) -> Result<Self> {
        if buffer.is_empty() {
            return Ok(GitFrame::Flush);
        }

        if get_value_for_key(b"git-filter", &buffer).is_some() {
            return Ok(GitFrame::Filter(GitFilterType::parse_buffer(&buffer)?));
        }
        if get_value_for_key(b"capability=", &buffer).is_some() {
            return Ok(GitFrame::Capability(GitCapability::parse_buffer(&buffer)?));
        }
        if get_value_for_key(b"status=", &buffer).is_some() {
            return Ok(GitFrame::Status(GitStatus::parse_buffer(&buffer)?));
        }
        if get_value_for_key(b"version=", &buffer).is_some() {
            return Ok(GitFrame::Version(Self::parse_version(&buffer)?));
        }
        if get_value_for_key(b"command=", &buffer).is_some() {
            return Ok(GitFrame::Command(GitCommand::parse_buffer(&buffer)?));
        }
        if get_value_for_key(b"can-delay=", &buffer).is_some() {
            return Ok(GitFrame::CanDelay(Self::parse_can_delay(&buffer)?));
        }
        if let Some(value) = get_value_for_key(b"pathname=", &buffer) {
            let mut last = buffer.len();
            if buffer.last().copied() == Some(b'\n') {
                last -= 1;
            }

            let str_dest = std::str::from_utf8(&buffer[value..last]).map_err(|e| {
                GitXetRepoError::StreamParseError(format!("Parse error: bad UTF-8 ({e:?})"))
            })?;
            return Ok(GitFrame::PathInfo(str_dest.to_string()));
        }

        // otherwise, it's a data block.
        Ok(GitFrame::Data(buffer))
    }

    /// Only returns a Data frame or a Flush frame, ignores the contents of the frame
    pub fn parse_data_or_flush_frame(buffer: Vec<u8>) -> Result<Self> {
        if buffer.is_empty() {
            return Ok(GitFrame::Flush);
        }
        Ok(GitFrame::Data(buffer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_version() {
        assert_eq!(Ok(2), GitFrame::parse_version(b"version=2".as_ref()));
        assert_eq!(Ok(2), GitFrame::parse_version(b"version=2\n".as_ref()));
        assert_eq!(Ok(0), GitFrame::parse_version(b"version=0".as_ref()));
        assert_eq!(Ok(1), GitFrame::parse_version(b"version=1".as_ref()));
        assert_eq!(Ok(24), GitFrame::parse_version(b"version=24".as_ref()));
        assert_eq!(Ok(342), GitFrame::parse_version(b"version=342".as_ref()));
        assert!(GitFrame::parse_version(b"version=foo".as_ref()).is_err());
    }

    #[test]
    fn test_git_status_conversion() {
        assert_eq!("status=abort", format!("{}", GitStatus::Abort));
        assert_eq!("status=delayed", format!("{}", GitStatus::Delayed));
        assert_eq!("status=error", format!("{}", GitStatus::Error));
        assert_eq!("status=success", format!("{}", GitStatus::Success));

        assert_eq!(
            Ok(GitStatus::Abort),
            GitStatus::parse_buffer(b"status=abort".as_ref())
        );
        assert_eq!(
            Ok(GitStatus::Abort),
            GitStatus::parse_buffer(b"status=abort\n".as_ref())
        );
        assert_eq!(
            Ok(GitStatus::Delayed),
            GitStatus::parse_buffer(b"status=delayed".as_ref())
        );
        assert_eq!(
            Ok(GitStatus::Delayed),
            GitStatus::parse_buffer(b"status=delayed\n".as_ref())
        );
        assert_eq!(
            Ok(GitStatus::Error),
            GitStatus::parse_buffer(b"status=error".as_ref())
        );
        assert_eq!(
            Ok(GitStatus::Error),
            GitStatus::parse_buffer(b"status=error\n".as_ref())
        );
        assert_eq!(
            Ok(GitStatus::Success),
            GitStatus::parse_buffer(b"status=success".as_ref())
        );
        assert_eq!(
            Ok(GitStatus::Success),
            GitStatus::parse_buffer(b"status=success\n".as_ref())
        );

        assert!(GitStatus::parse_buffer(b"foo bar".as_ref()).is_err());
        assert!(GitStatus::parse_buffer(b"status=foo".as_ref()).is_err());
    }

    #[test]
    fn test_git_filter_type() {
        assert_eq!("git-filter-client", format!("{}", GitFilterType::Client));
        assert_eq!("git-filter-server", format!("{}", GitFilterType::Server));

        assert_eq!(
            Ok(GitFilterType::Client),
            GitFilterType::parse_buffer(b"git-filter-client\n".as_ref())
        );
        assert_eq!(
            Ok(GitFilterType::Client),
            GitFilterType::parse_buffer(b"git-filter-client".as_ref())
        );
        assert_eq!(
            Ok(GitFilterType::Server),
            GitFilterType::parse_buffer(b"git-filter-server\n".as_ref())
        );
        assert_eq!(
            Ok(GitFilterType::Server),
            GitFilterType::parse_buffer(b"git-filter-server".as_ref())
        );
        assert!(GitFilterType::parse_buffer(b"git-filter-foo".as_ref()).is_err());
        assert!(GitFilterType::parse_buffer(b"git-filter-unsupported".as_ref()).is_err());
        assert!(GitFilterType::parse_buffer(b"git-filter-clie foo".as_ref()).is_err());
    }

    #[test]
    fn test_git_capability() {
        assert_eq!("capability=clean", format!("{}", GitCapability::Clean));
        assert_eq!("capability=smudge", format!("{}", GitCapability::Smudge));
        assert_eq!("capability=delay", format!("{}", GitCapability::Delay));

        assert_eq!(
            Ok(GitCapability::Clean),
            GitCapability::parse_buffer(b"capability=clean\n".as_ref())
        );
        assert_eq!(
            Ok(GitCapability::Clean),
            GitCapability::parse_buffer(b"capability=clean".as_ref())
        );
        assert_eq!(
            Ok(GitCapability::Smudge),
            GitCapability::parse_buffer(b"capability=smudge\n".as_ref())
        );
        assert_eq!(
            Ok(GitCapability::Smudge),
            GitCapability::parse_buffer(b"capability=smudge".as_ref())
        );
        assert_eq!(
            Ok(GitCapability::Delay),
            GitCapability::parse_buffer(b"capability=delay\n".as_ref())
        );
        assert_eq!(
            Ok(GitCapability::Delay),
            GitCapability::parse_buffer(b"capability=delay".as_ref())
        );
        assert!(GitCapability::parse_buffer(br"foo\n".as_ref()).is_err());
        assert!(GitCapability::parse_buffer(br"capability=foo\n".as_ref()).is_err());
        assert!(GitCapability::parse_buffer(br"capability=clea foo\n".as_ref()).is_err());
    }

    #[test]
    fn test_git_command() {
        assert_eq!("command=clean", format!("{}", GitCommand::Clean));
        assert_eq!("command=smudge", format!("{}", GitCommand::Smudge));

        assert_eq!(
            Ok(GitCommand::Clean),
            GitCommand::parse_buffer(b"command=clean\n".as_ref())
        );
        assert_eq!(
            Ok(GitCommand::Clean),
            GitCommand::parse_buffer(b"command=clean".as_ref())
        );
        assert_eq!(
            Ok(GitCommand::Smudge),
            GitCommand::parse_buffer(b"command=smudge\n".as_ref())
        );
        assert_eq!(
            Ok(GitCommand::Smudge),
            GitCommand::parse_buffer(b"command=smudge".as_ref())
        );
        assert!(GitCommand::parse_buffer(br"foo\n".as_ref()).is_err());
        assert!(GitCommand::parse_buffer(br"command=foo\n".as_ref()).is_err());
        assert!(GitCommand::parse_buffer(br"command=clea foo\n".as_ref()).is_err());
    }

    #[test]
    fn test_git_frame_parse() {
        // filter type
        assert_eq!(
            Ok(GitFrame::Filter(GitFilterType::Server)),
            GitFrame::parse_frame(b"git-filter-server\n".to_vec())
        );
        assert_eq!(
            Ok(GitFrame::Filter(GitFilterType::Client)),
            GitFrame::parse_frame(b"git-filter-client\n".to_vec())
        );

        // versions
        assert_eq!(
            Ok(GitFrame::Version(42)),
            GitFrame::parse_frame(b"version=42\n".to_vec())
        );
        assert_eq!(
            Ok(GitFrame::Version(0)),
            GitFrame::parse_frame(b"version=0\n".to_vec())
        );

        assert_eq!(
            Ok(GitFrame::Version(0)),
            GitFrame::parse_frame(b"version=0\n".to_vec())
        );

        assert_eq!(
            Ok(GitFrame::CanDelay(0)),
            GitFrame::parse_frame(b"can-delay=0\n".to_vec())
        );
        assert_eq!(
            Ok(GitFrame::CanDelay(1)),
            GitFrame::parse_frame(b"can-delay=1\n".to_vec())
        );

        // capabilities
        assert_eq!(
            Ok(GitFrame::Capability(GitCapability::Smudge)),
            GitFrame::parse_frame(b"capability=smudge\n".to_vec())
        );
        assert_eq!(
            Ok(GitFrame::Capability(GitCapability::Clean)),
            GitFrame::parse_frame(b"capability=clean\n".to_vec())
        );
        assert_eq!(
            Ok(GitFrame::Capability(GitCapability::Delay)),
            GitFrame::parse_frame(b"capability=delay\n".to_vec())
        );

        // status
        assert_eq!(
            Ok(GitFrame::Status(GitStatus::Success)),
            GitFrame::parse_frame(b"status=success\n".to_vec())
        );
        assert_eq!(
            Ok(GitFrame::Status(GitStatus::Abort)),
            GitFrame::parse_frame(b"status=abort\n".to_vec())
        );
        assert_eq!(
            Ok(GitFrame::Status(GitStatus::Error)),
            GitFrame::parse_frame(b"status=error\n".to_vec())
        );

        // command
        assert_eq!(
            Ok(GitFrame::Command(GitCommand::Clean)),
            GitFrame::parse_frame(b"command=clean\n".to_vec())
        );
        assert_eq!(
            Ok(GitFrame::Command(GitCommand::Smudge)),
            GitFrame::parse_frame(b"command=smudge\n".to_vec())
        );

        // path
        assert_eq!(
            Ok(GitFrame::PathInfo("/foo/bar".to_string())),
            GitFrame::parse_frame(b"pathname=/foo/bar\n".to_vec())
        );

        // flush
        assert_eq!(Ok(GitFrame::Flush), GitFrame::parse_frame(b"".to_vec()));

        let data = b"capbility\nfilter\nsmudge\ncommand\nstatus";
        assert_eq!(
            Ok(GitFrame::Data(data.to_vec())),
            GitFrame::parse_frame(data.to_vec())
        );
    }

    #[test]
    fn test_git_frame_parse_failure() {
        // check invalid versions of our key-value pairs
        let res = GitFrame::parse_frame(b"command=foo".to_vec());
        assert!(res.is_err());

        let res = GitFrame::parse_frame(b"capability=foo".to_vec());
        assert!(res.is_err());

        let res = GitFrame::parse_frame(b"version=foo".to_vec());
        assert!(res.is_err());

        let res = GitFrame::parse_frame(b"status=foo".to_vec());
        assert!(res.is_err());

        // check our UTF-8 processing errors
        let invalid_utf8 = vec![b'\xe2', b'\x28', b'\xa1'];

        assert!(
            GitFrame::parse_frame([b"pathname=".to_vec(), invalid_utf8.clone()].concat()).is_err()
        );
        assert!(
            GitFrame::parse_frame([b"command=".to_vec(), invalid_utf8.clone()].concat()).is_err()
        );
        assert!(
            GitFrame::parse_frame([b"capability=".to_vec(), invalid_utf8.clone()].concat())
                .is_err()
        );
        assert!(
            GitFrame::parse_frame([b"status=".to_vec(), invalid_utf8.clone()].concat()).is_err()
        );
        assert!(GitFrame::parse_frame([b"version=".to_vec(), invalid_utf8].concat()).is_err());
    }
}
