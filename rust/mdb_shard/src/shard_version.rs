use std::{fmt, path::Path, str::FromStr};

use crate::error::{MDBShardError, Result};

pub const MDB_SHARD_VERSION: u64 = 2;
pub const MDB_SHARD_HEADER_VERSION: u64 = MDB_SHARD_VERSION;
pub const MDB_SHARD_FOOTER_VERSION: u64 = MDB_SHARD_VERSION;

#[derive(PartialEq, Eq, PartialOrd, Ord, Copy, Clone, Debug, Default)]
pub enum MDBShardVersion {
    // Use MerkleMemDB
    V1 = 1,
    // Use MDBShardInfo
    #[default]
    V2,
    // Future versions can be added to this enum
}

impl TryFrom<u64> for MDBShardVersion {
    type Error = MDBShardError;

    fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            2 => Ok(Self::V2),
            _ => Err(MDBShardError::MDBShardVersionError(format!(
                "{} is not a valid version",
                value
            ))),
        }
    }
}

impl FromStr for MDBShardVersion {
    type Err = MDBShardError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let v = s.parse::<u64>().map_err(|_| {
            MDBShardError::MDBShardVersionError(format!("{} is not a valid version", s))
        })?;
        MDBShardVersion::try_from(v)
    }
}

impl fmt::Display for MDBShardVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let v = *self as u64;
        write!(f, "{v}")
    }
}

impl MDBShardVersion {
    pub fn try_from_file(path: impl AsRef<Path>) -> Result<Self> {
        std::fs::read_to_string(path)?.parse::<Self>()
    }

    pub fn get_value(&self) -> u64 {
        *self as u64
    }

    pub fn get_lower(&self) -> Option<Self> {
        MDBShardVersion::try_from(*self as u64 - 1).ok()
    }

    pub fn need_salt(&self) -> bool {
        match self {
            MDBShardVersion::V1 => false,
            MDBShardVersion::V2 => true,
        }
    }

    pub fn get_max() -> MDBShardVersion {
        Self::try_from(MDB_SHARD_VERSION).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::shard_version::{MDBShardVersion, MDB_SHARD_VERSION};

    use crate::error::*;

    use std::str::FromStr;
    use tempfile::TempDir;

    #[test]
    fn test_from_u64() -> Result<()> {
        assert_eq!(MDBShardVersion::try_from(1)?, MDBShardVersion::V1);
        assert_eq!(MDBShardVersion::try_from(2)?, MDBShardVersion::V2);
        assert!(MDBShardVersion::try_from(0).is_err());

        Ok(())
    }

    #[test]
    fn test_from_string() -> Result<()> {
        assert_eq!(MDBShardVersion::from_str("1")?, MDBShardVersion::V1);
        assert_eq!(MDBShardVersion::from_str("2")?, MDBShardVersion::V2);
        assert!(MDBShardVersion::from_str("0").is_err());
        assert!(MDBShardVersion::from_str("text").is_err());

        Ok(())
    }

    #[test]
    fn test_from_file() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let v = MDBShardVersion::V1;
        let file_name = tmp_dir.path().join("version.lock");

        std::fs::write(&file_name, v.to_string())?;

        let r = MDBShardVersion::try_from_file(&file_name)?;

        assert_eq!(v, r);

        Ok(())
    }

    #[test]
    fn test_get_lower() -> Result<()> {
        assert_eq!(MDBShardVersion::V2.get_lower(), Some(MDBShardVersion::V1));
        assert_eq!(MDBShardVersion::V1.get_lower(), None);

        Ok(())
    }

    #[test]
    fn test_get_max() -> Result<()> {
        assert_eq!(MDBShardVersion::get_max().get_value(), MDB_SHARD_VERSION);

        Ok(())
    }
}
