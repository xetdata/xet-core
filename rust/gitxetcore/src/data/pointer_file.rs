#![cfg_attr(feature = "strict", deny(warnings))]
use merklehash::{DataHashHexParseError, MerkleHash};
use static_assertions::const_assert;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::BufWriter,
    path::Path,
};
use toml::Value;
use tracing::{debug, error, warn};

use super::PointerFileTranslator;
use crate::errors::Result;
use crate::{constants::POINTER_FILE_LIMIT, stream::data_iterators::AsyncDataIterator};

const HEADER_PREFIX: &str = "# xet version ";
const CURRENT_VERSION: &str = "0";

/// A struct that wraps a Xet pointer file.
/// Xet pointer file format is a TOML file,
/// and the first line must be of the form "# xet version <x.y>"
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointerFile {
    /// The version string of the pointer file
    version_string: String,

    /// The initial path supplied (to a pointer file on disk)
    path: String,

    /// Whether the contents represent a valid pointer file.
    /// is_valid is true if and only if all of the following are true:
    /// * the first line starts with HEADER_PREFIX and then a version string
    /// * the whole contents are valid TOML
    /// * the TOML contains a top level key "hash" that is a String
    /// * the TOML contains a top level key "filesize" that is an Integer
    is_valid: bool,

    /// The Merkle hash of the file pointed to by this pointer file
    hash: String,

    /// The size of the file pointed to by this pointer file
    filesize: u64,
}

impl PointerFile {
    pub fn init_from_string(contents: &str, path: &str) -> PointerFile {
        let empty_string = "".to_string();

        // Start out valid by default.
        let mut is_valid = true;

        // Required members: hash and filesize.
        // Without these, not considered valid.
        let mut hash = empty_string.clone();
        let mut filesize: u64 = 0;

        let lines = contents.lines();
        let first_line: String = lines.take(1).collect();
        if !first_line.starts_with(HEADER_PREFIX) {
            // not a valid pointer file - doesn't start with header:
            // # xet version <x.y>
            is_valid = false;
            return PointerFile {
                version_string: empty_string,
                path: path.to_string(),
                is_valid,
                hash,
                filesize,
            };
        }

        let version_string = first_line[HEADER_PREFIX.len()..].to_string();
        if version_string != CURRENT_VERSION {
            warn!("Pointer file version {} encountered. Only version {} is supported. Please upgrade git-xet.", version_string, CURRENT_VERSION);
            // not a valid pointer file, doesn't start with header + version string
            is_valid = false;
            return PointerFile {
                version_string,
                path: path.to_string(),
                is_valid,
                hash,
                filesize,
            };
        }

        // Validated the header -- parse as TOML.
        let parsed = match contents.parse::<Value>() {
            Ok(v) => v,
            Err(_) => {
                is_valid = false;
                Value::String(empty_string)
            }
        };

        match parsed.get("hash") {
            Some(Value::String(s)) => {
                hash = s.to_string();
            }
            _ => {
                // did not find hash, or
                // found a non-string type for hash (unexpected)
                is_valid = false;
            }
        }

        match parsed.get("filesize") {
            Some(Value::Integer(i)) => {
                if *i < 0 {
                    // negative int should not be possible for filesize
                    is_valid = false;
                }
                filesize = *i as u64;
            }
            _ => {
                // did not find filesize, or
                // found a non-int type for filesize (unexpected)
                is_valid = false;
            }
        }

        PointerFile {
            version_string,
            path: path.to_string(),
            is_valid,
            hash,
            filesize,
        }
    }

    /// Initialize a pointer file by the contents in the file.
    /// This will quickly check the file size before trying to read the
    /// entire file. Any I/O failure or file size exceeding a limit means
    /// an invalid pointer file.
    pub fn init_from_path(path: impl AsRef<Path>) -> PointerFile {
        let path = path.as_ref().to_str().unwrap();
        let empty_string = "".to_string();

        let invalid_pointer_file = || PointerFile {
            version_string: empty_string.clone(),
            path: path.to_owned(),
            is_valid: false,
            hash: empty_string,
            filesize: 0,
        };

        let Ok(file_meta) = fs::metadata(path).map_err(|e| {
            debug!("fs:metadata failed: {e:?}");
            e
        }) else {
            return invalid_pointer_file();
        };
        if file_meta.len() > POINTER_FILE_LIMIT as u64 {
            debug!("filesize: {}", file_meta.len());
            return invalid_pointer_file();
        }
        let Ok(contents) = fs::read_to_string(path).map_err(|e| {
            debug!("fs:read_to_string failed: {e:?}");
            e
        }) else {
            return invalid_pointer_file();
        };

        PointerFile::init_from_string(&contents, path)
    }

    pub fn init_from_info(path: &str, hash: &str, filesize: u64) -> Self {
        Self {
            version_string: CURRENT_VERSION.to_string(),
            path: path.to_string(),
            is_valid: true,
            hash: hash.to_string(),
            filesize,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.is_valid
    }

    pub fn hash_string(&self) -> &String {
        &self.hash
    }

    pub fn hash(&self) -> std::result::Result<MerkleHash, DataHashHexParseError> {
        if self.is_valid {
            MerkleHash::from_hex(&self.hash).map_err(|e| {
                error!(
                    "Error parsing hash value in pointer file for {:?}: {e:?}",
                    self.path
                );
                e
            })
        } else {
            Ok(MerkleHash::default())
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn filesize(&self) -> u64 {
        self.filesize
    }
}

pub fn is_xet_pointer_file(data: &[u8]) -> bool {
    if data.len() >= POINTER_FILE_LIMIT {
        return false;
    }

    let Ok(data_str) = std::str::from_utf8(data) else {
        return false;
    };

    PointerFile::init_from_string(data_str, "").is_valid()
}

impl std::fmt::Display for PointerFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.is_valid {
            warn!("called fmt on invalid PointerFile");
            return write!(f, "# invalid pointer file");
        }
        let mut contents = BTreeMap::<String, Value>::new();
        contents.insert("hash".to_string(), Value::String(self.hash.clone()));
        assert!(self.filesize <= i64::MAX as u64);
        contents.insert("filesize".to_string(), Value::Integer(self.filesize as i64));
        let contents_str = toml::ser::to_string_pretty(&contents).map_err(|e| {
            warn!("Error serializing pointer file: {e}:");
            std::fmt::Error
        })?;

        assert!(!self.version_string.is_empty());
        write!(
            f,
            "{}{}\n{}",
            HEADER_PREFIX, self.version_string, contents_str
        )
    }
}

// Check pointer file size limit at compile time.
// A valid pointer file looks like below
//
// # xet version 0
// filesize = <i64 number>
// hash = '<64 digit long string>'
//
//
const_assert!(
    POINTER_FILE_LIMIT
        >= HEADER_PREFIX.len() + CURRENT_VERSION.len() // header
+ "filesize = ".len() + "9223372036854775807".len() // the largest i64
+ "hash = ".len() + 64 + 2 // 2 is the single quotes size
+ 2 * 3 // 3 "\n" or "\r\n" on Windows
);

/// Tries to parse a pointer file from the reader, but if parsing fails, returns
/// the data we have pulled out from the reader. (The AsyncIterator does not
/// provide a "put-back" function and for simplicity it probably shouldn't
/// have that).
///
/// if a pointer file is parsed successfully Returns `Ok(Some(PointerFile), data)`
/// if a pointer file is parsed unsuccessfully, Returns `Ok(None, data)`
/// if the read stream failed for reasons which are not EOF, returns `Err(e)`
pub async fn pointer_file_from_reader(
    path: &Path,
    reader: &mut impl AsyncDataIterator,
    force_no_smudge: bool,
) -> Result<(Option<PointerFile>, Vec<u8>)> {
    let mut data: Vec<u8> = Vec::new();

    while data.len() < POINTER_FILE_LIMIT {
        match reader.next().await? {
            Some(mut new_data) => {
                data.append(&mut new_data);
            }
            None => {
                break;
            }
        };
    }

    if force_no_smudge || data.len() > POINTER_FILE_LIMIT {
        // too large.
        return Ok((None, data));
    }

    let file_str: &str = match std::str::from_utf8(&data[..]) {
        Ok(v) => v,
        Err(_) => return Ok((None, data)), // can't utf-8. definitely a bad pointer file
    };

    let ptr_file = PointerFile::init_from_string(file_str, path.to_str().unwrap());
    if ptr_file.is_valid() {
        Ok((Some(ptr_file), data))
    } else {
        // pointer file did not parse correctly
        Ok((None, data))
    }
}

/// Smudge a pointer file and overwrite itself
pub async fn smudge_pointerfile_to_itself(
    translator: &PointerFileTranslator,
    path: &Path,
) -> anyhow::Result<()> {
    let pointer_file = PointerFile::init_from_path(path);

    // not a pointer file, leave it as it is.
    if !pointer_file.is_valid() {
        return Ok(());
    }

    let file_hash = pointer_file.hash()?;

    // Create a temporary path for writing to, then move it over when done.

    let mut writer = Box::new(BufWriter::new(File::create(path)?));

    translator
        .smudge_file_from_hash(Some(path.to_owned()), &file_hash, &mut writer, None)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    const POINTER_FILE_VERSION: &str = "0";
    use super::*;

    #[test]
    fn is_valid_pointer_file() {
        let empty_string = "".to_string();
        let mut test_contents = "# not a xet file\n42 is a number".to_string();
        let mut test = PointerFile::init_from_string(&test_contents, &empty_string);
        assert!(!test.is_valid()); // not valid because it is missing the header prefix

        test_contents = format!("{}{}\n42 is a number", HEADER_PREFIX, POINTER_FILE_VERSION);
        test = PointerFile::init_from_string(&test_contents, &empty_string);
        assert!(!test.is_valid()); // not valid because it doesn't contain valid TOML

        test_contents = format!("{}{}\nfoo = 'bar'", HEADER_PREFIX, POINTER_FILE_VERSION);
        test = PointerFile::init_from_string(&test_contents, &empty_string);
        assert!(!test.is_valid()); // not valid because it doesn't contain hash or filesize

        test_contents = format!(
            "{}{}\nhash = '12345'\nfilesize = 678",
            HEADER_PREFIX, POINTER_FILE_VERSION
        );
        test = PointerFile::init_from_string(&test_contents, &empty_string);
        assert!(test.is_valid()); // valid
    }

    #[test]
    fn empty_file() {
        let empty_string = "".to_string();
        let test = PointerFile::init_from_string(&empty_string, &empty_string);
        assert!(!test.is_valid()); // not valid because empty file
    }

    #[test]
    fn parses_correctly() {
        let empty_string = "".to_string();
        let test_contents = format!(
            "{}{}\nhash = '12345'\nfilesize = 678",
            HEADER_PREFIX, POINTER_FILE_VERSION
        );
        let test = PointerFile::init_from_string(&test_contents, &empty_string);
        assert!(test.is_valid()); // valid
        assert_eq!(test.filesize(), 678);
        assert_eq!(test.hash_string(), "12345");
        assert_eq!(test.version_string, POINTER_FILE_VERSION);
    }

    #[test]
    fn is_serializable_and_deserializable() {
        let empty_string = "".to_string();
        let test_contents = format!(
            "{}{}\nhash = '12345'\nfilesize = 678",
            HEADER_PREFIX, POINTER_FILE_VERSION
        );
        let test = PointerFile::init_from_string(&test_contents, &empty_string);
        assert!(test.is_valid()); // valid

        // make sure we can serialize it back out to string
        let serialized = test.to_string();

        // then read it back in, and make sure it's equal to the original
        let deserialized = PointerFile::init_from_string(&serialized, &empty_string);
        assert_eq!(test, deserialized);
    }

    #[test]
    fn test_new_version() {
        let empty_string = "".to_string();
        let test_contents = format!("{}{}\nhash = '12345'\nfilesize = 678", HEADER_PREFIX, "1.0");
        let test = PointerFile::init_from_string(&test_contents, &empty_string);
        assert!(!test.is_valid()); // new version is not valid
    }
}
