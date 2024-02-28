use safe_transmute::{transmute_many_permissive, transmute_to_bytes};
use serde::{Deserialize, Serialize};
use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};
use std::error::Error;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::mem::transmute_copy;
use std::num::ParseIntError;
use std::ops::{Deref, DerefMut};
use std::str;
use std::u64;

/**************************************************************************/
/*                                                                        */
/*                                DataHash                                */
/*                                                                        */
/**************************************************************************/

/// The DataHash is a transparent 256-bit value stores as `[u64; 4]`.
///
/// [compute_data_hash] and [compute_internal_node_hash] are the two main
/// ways in which a hash on data can be computed.
///
/// Many convenient trait implementations are provided for printing, comparing,
/// and parsing.
///
/// ```ignore
/// let string = "hello world";
/// let hash = compute_data_hash(slice.as_bytes());
/// println!("Hello Hash {}", hash);
/// ```
#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct DataHash([u64; 4]);

impl Deref for DataHash {
    type Target = [u64; 4];
    #[inline(always)]
    fn deref(&self) -> &[u64; 4] {
        &self.0
    }
}

impl DerefMut for DataHash {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [u64; 4] {
        &mut (self.0)
    }
}

impl From<[u64; 4]> for DataHash {
    fn from(value: [u64; 4]) -> Self {
        DataHash(value)
    }
}

impl From<[u8; 32]> for DataHash {
    fn from(value: [u8; 32]) -> Self {
        unsafe { Self(transmute_copy::<[u8; 32], [u64; 4]>(&value)) }
    }
}

impl From<&[u8; 32]> for DataHash {
    fn from(value: &[u8; 32]) -> Self {
        unsafe { Self(transmute_copy::<[u8; 32], [u64; 4]>(value)) }
    }
}

impl AsRef<[u8]> for DataHash {
    fn as_ref(&self) -> &[u8] {
        transmute_to_bytes(self.deref())
    }
}

impl Default for DataHash {
    /// The default constructor returns a DataHash of 0s
    fn default() -> DataHash {
        DataHash([0; 4])
    }
}

impl PartialEq for DataHash {
    fn eq(&self, other: &Self) -> bool {
        self[0] == other[0] && self[1] == other[1] && self[2] == other[2] && self[3] == other[3]
    }
}

impl Eq for DataHash {}

impl Ord for DataHash {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for DataHash {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl core::ops::Rem<u64> for DataHash {
    type Output = u64;

    fn rem(self, rhs: u64) -> Self::Output {
        self[3] % rhs
    }
}

unsafe impl heed::bytemuck::Zeroable for DataHash {
    fn zeroed() -> Self {
        DataHash([0; 4])
    }
}
unsafe impl heed::bytemuck::Pod for DataHash {}

/// The error type that is returned if [DataHash::from_hex] fails.
#[derive(Debug, Clone)]
pub struct DataHashHexParseError;

impl Error for DataHashHexParseError {}

impl fmt::Display for DataHashHexParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid hex input for DataHash")
    }
}

impl From<ParseIntError> for DataHashHexParseError {
    fn from(_err: ParseIntError) -> Self {
        DataHashHexParseError {}
    }
}

impl DataHash {
    /// Returns the hexadecimal printout of the hash.
    pub fn hex(&self) -> String {
        format!(
            "{:016x}{:016x}{:016x}{:016x}",
            self.0[0], self.0[1], self.0[2], self.0[3]
        )
    }

    /// Parses a hexadecimal string as a DataHash, returning
    /// Err(DataHashHexParseError) on failure.
    pub fn from_hex(h: &str) -> Result<DataHash, DataHashHexParseError> {
        if h.len() != 64 {
            return Err(DataHashHexParseError {});
        }
        let mut ret: DataHash = Default::default();

        let good = h.as_bytes().iter().all(|c| c.is_ascii_hexdigit());
        if !good {
            return Err(DataHashHexParseError {});
        }
        ret.0[0] = u64::from_str_radix(&h[..16], 16)?;
        ret.0[1] = u64::from_str_radix(&h[16..32], 16)?;
        ret.0[2] = u64::from_str_radix(&h[32..48], 16)?;
        ret.0[3] = u64::from_str_radix(&h[48..64], 16)?;
        Ok(ret)
    }

    /// Returns the datahash as a raw byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        transmute_to_bytes(&self.0[..])
    }
}

/// The error type that is returned if TryFrom<&[u8]> fails.
#[derive(Debug, Clone)]
pub struct DataHashBytesParseError;

impl Error for DataHashBytesParseError {}

impl fmt::Display for DataHashBytesParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid bytes input for DataHash")
    }
}

impl TryFrom<&[u8]> for DataHash {
    type Error = DataHashBytesParseError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != 32 {
            return Err(DataHashBytesParseError);
        }
        let mut hash: DataHash = DataHash::default();
        hash.copy_from_slice(transmute_many_permissive::<u64>(value).unwrap());
        Ok(hash)
    }
}

impl From<DataHash> for Vec<u8> {
    fn from(val: DataHash) -> Self {
        val.as_bytes().into()
    }
}

impl From<&DataHash> for Vec<u8> {
    fn from(val: &DataHash) -> Self {
        val.as_bytes().into()
    }
}

// this is already a nice hash function. We just give the last 64-bits
// for use in hashtables etc.
impl Hash for DataHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.0[0]);
    }
}
// as generated from random.org
/// The hash key used for [compute_data_hash]
const DATA_KEY: [u8; 32] = [
    102, 151, 245, 119, 91, 149, 80, 222, 49, 53, 203, 172, 165, 151, 24, 28, 157, 228, 33, 16,
    155, 235, 43, 88, 180, 208, 176, 75, 147, 173, 242, 41,
];

/// The hash key used for [compute_internal_node_hash]
const INTERNAL_NODE_HASH: [u8; 32] = [
    1, 126, 197, 199, 165, 71, 41, 150, 253, 148, 102, 102, 180, 138, 2, 230, 93, 221, 83, 111, 55,
    199, 109, 210, 248, 99, 82, 230, 74, 83, 113, 63,
];

/// Hash function used to compute a leaf hash of the MerkleTree
/// from any user-provided sequence of bytes. You should be using
/// [compute_internal_node_hash] if this hash is to be used for interior
/// nodes.
///
/// Example:
/// ```ignore
/// let string = "hello world";
/// let hash = compute_data_hash(slice.as_bytes());
/// println!("Hello Hash {}", hash);
/// ```
pub fn compute_data_hash(slice: &[u8]) -> DataHash {
    let digest = blake3::keyed_hash(&DATA_KEY, slice);
    let mut hash: DataHash = DataHash::default();
    hash.copy_from_slice(transmute_many_permissive::<u64>(&digest.as_bytes()[..]).unwrap());
    hash
}

/// Hash function used to compute the hash of an interior node.
///
/// Note that this method also accepts a slice
/// of `&[u8]` and it is up to the caller to format the string appropriately.
/// For instance: the string could be simply the child hashes printed out
/// consecutively.
///
/// The reason why this method does not simply take an array of Hashes, and
/// instead require the caller to format the input as a string is to allow the
/// user to add additional information to the string being hashed (beyond just
/// the hashes itself). i.e. the string being hashed could be a concatenation
/// of "hashes of children + children metadata".
///
/// Example:
/// ```ignore
///     let mut buf = String::with_capacity(1024);
///     for node in nodes.iter() {
///         writeln!(buf, "{:x} : {}", node.hash(), node.len()).unwrap();
///     }
///     compute_internal_node_hash(buf.as_bytes())
/// ```
pub fn compute_internal_node_hash(slice: &[u8]) -> DataHash {
    let digest = blake3::keyed_hash(&INTERNAL_NODE_HASH, slice);
    let mut hash: DataHash = DataHash::default();
    hash.copy_from_slice(transmute_many_permissive::<u64>(&digest.as_bytes()[..]).unwrap());
    hash
}

impl fmt::LowerHex for DataHash {
    /// Allow the DataHash to be printed with
    /// `println!("{:x}", hash)`
    /// This prints the hexadecimal representation of the Hash.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.hex())
    }
}

impl fmt::Display for DataHash {
    /// Allow the DataHash to be printed with
    /// `println!("{}", hash)`
    /// This prints the hexadecimal representation of the Hash.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.hex())
    }
}
impl fmt::Debug for DataHash {
    /// Allow the DataHash to be printed with
    /// `println!("{:?}", hash)`
    /// This prints the hexadecimal representation of the Hash.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.hex())
    }
}

/// Wrapper around a Write trait that allows computation of the hash at the end.
///
/// It is recommended to wrap this in a BufWriter object: i.e.
///
/// let out_file = std::fs::OpenOptions::new().create(true).write(true).open("temp.fs")?;
///
/// let hashed_write = HashedWrite::new(out_file);
///
/// {
///    let buf_writer = BufWriter::new(&mut hashed_write);
///
///    // Do the writing against buf_writer
///
///    
/// }
///
/// let h = hashed_write.hash();
///  
pub struct HashedWrite<W: Write> {
    hasher: blake3::Hasher,
    writer: W,
}

impl<W: Write> HashedWrite<W> {
    pub fn new(writer: W) -> Self {
        Self {
            hasher: blake3::Hasher::new_keyed(&DATA_KEY),
            writer,
        }
    }

    pub fn hash(&self) -> DataHash {
        let digest = self.hasher.finalize();
        let mut hash: DataHash = DataHash::default();
        hash.copy_from_slice(transmute_many_permissive::<u64>(&digest.as_bytes()[..]).unwrap());
        hash
    }
}

impl<W: Write> Write for HashedWrite<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        self.writer.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use rand::prelude::*;
    use std::io::Write;

    use crate::{compute_data_hash, DataHash, HashedWrite};

    #[test]
    fn test_try_from_bytes() {
        let hash_bytes_proper = [1u8; 32].to_vec();
        assert!(DataHash::try_from(hash_bytes_proper.as_slice()).is_ok());

        let hash_bytes_improper = [1u8; 31];
        assert!(DataHash::try_from(hash_bytes_improper.as_slice()).is_err());
    }

    #[test]
    fn test_hashed_write() -> std::io::Result<()> {
        let mut written_data = Vec::<u8>::with_capacity(300);
        let mut raw_data = vec![0u8; 300];

        let mut rng = StdRng::seed_from_u64(0);
        rng.fill_bytes(&mut raw_data[..]);

        let mut hashed_write = HashedWrite::new(&mut written_data);

        for i in 0..30 {
            hashed_write.write_all(&raw_data[(10 * i)..(10 * (i + 1))])?;
        }

        assert_eq!(hashed_write.hash(), compute_data_hash(&raw_data[..]));
        assert_eq!(written_data, raw_data);

        Ok(())
    }
}
