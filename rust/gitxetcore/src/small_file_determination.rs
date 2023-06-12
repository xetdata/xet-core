/// Implements the small file heuristic to determine if this file
/// should be stored in Git or MerkleDB.
/// The current heuristic is simple:
///  - if it is less than the SMALL_FILE_THRESHOLD
///    AND if it decodes as utf-8, it is a small file
pub fn is_small_file(buf: &[u8], small_file_threshold: usize) -> bool {
    buf.len() < small_file_threshold && std::str::from_utf8(buf).is_ok()
}
