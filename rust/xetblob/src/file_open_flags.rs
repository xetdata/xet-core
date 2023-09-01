pub type FileOpenFlags = u32;
// Flag definitions borrow from https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilea

pub const FILE_FLAG_DEFAULT: u32 = 0x0;
// The file is being opened with no caching for data reads and writes.
// This flag does not affect hard disk caching.
pub const FILE_FLAG_NO_BUFFERING: u32 = 0x20000000;
