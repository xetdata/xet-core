use crate::errors::Result;
use crate::stream::data_iterators::AsyncDataIterator;

/// Implements the small file heuristic to determine if this file
/// should be stored in Git or MerkleDB.
/// The current heuristic is simple:
///  - if it is less than the SMALL_FILE_THRESHOLD
///    AND if it decodes as utf-8, it is a small file
pub fn is_file_passthrough(buf: &[u8], small_file_threshold: usize) -> bool {
    buf.len() < small_file_threshold && std::str::from_utf8(buf).is_ok()
}

pub fn is_possible_start_to_text_file(buf: &[u8]) -> bool {
    // In UTF-8 encoding, the maximum length of a character is 4 bytes. This means
    // that a valid UTF-8 character can span up to 4 bytes. Therefore, when you have
    // a sequence of bytes that could be part of a valid UTF-8 string but is truncated,
    // the maximum difference between the end of the buffer and the position indicated
    // by e.valid_up_to() from a Utf8Error is 3 bytes.
    match std::str::from_utf8(buf) {
        Ok(_) => true,
        Err(e) => e.valid_up_to() != 0 && e.valid_up_to() >= buf.len().saturating_sub(3),
    }
}

pub enum PassThroughFileStatus {
    PassFileThrough(Vec<u8>),
    ChunkFile(Vec<Vec<u8>>),
}

pub async fn check_passthrough_status(
    reader: &mut impl AsyncDataIterator,
    small_file_threshold: usize,
) -> Result<PassThroughFileStatus> {
    if small_file_threshold == 0 {
        return Ok(PassThroughFileStatus::ChunkFile(Vec::new()));
    }

    // we consume up to SMALL_FILE_THRESHOLD
    let mut tempbuf: Vec<Vec<u8>> = Vec::new();
    let mut readlen: usize = 0;
    let mut eofed: bool = false;

    // Do one pass to see if it's possibly a start to a text file.  If not, we can break early.
    match reader.next().await? {
        Some(buf) => {
            readlen += buf.len();
            tempbuf.push(buf);
        }
        None => {
            eofed = true;
        }
    }
    // Handle the zero file case.
    if tempbuf.is_empty() {
        return Ok(PassThroughFileStatus::PassFileThrough(vec![]));
    }

    if !is_possible_start_to_text_file(&tempbuf[0]) {
        return Ok(PassThroughFileStatus::ChunkFile(tempbuf));
    }

    // It could be a text file, so pull in data til we hit the small file threshhold, then test again.
    while readlen < small_file_threshold {
        match reader.next().await? {
            Some(buf) => {
                readlen += buf.len();
                tempbuf.push(buf);
            }
            None => {
                eofed = true;
                break;
            }
        }
    }

    if eofed && readlen < small_file_threshold {
        // It could be a text file, but need to get all the data
        // to make sure.
        let mut ret_data = Vec::<u8>::with_capacity(readlen);

        for it in tempbuf.iter() {
            ret_data.extend(it);
        }

        if is_file_passthrough(&ret_data, small_file_threshold) {
            Ok(PassThroughFileStatus::PassFileThrough(ret_data))
        } else {
            Ok(PassThroughFileStatus::ChunkFile(tempbuf))
        }
    } else {
        Ok(PassThroughFileStatus::ChunkFile(tempbuf))
    }
}
