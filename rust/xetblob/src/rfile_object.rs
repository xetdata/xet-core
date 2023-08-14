use std::{
    io::{BufWriter, Write},
    path::Path,
};

use gitxetcore::data_processing::*;
use pointer_file::PointerFile;

/// The channel limit for the write side of the handler.
/// The smudge task may write messages up to 16 MB,
/// and the default pyxet concurrent write limit is 32;
/// this limits the single channel volume to 128 MB, and total
/// channel volumn to 4 GB.
const BLOB_WRITE_MPSC_CHANNEL_SIZE: usize = 8;

#[derive(Clone)]
pub enum FileContent {
    Pointer((PointerFile, MiniPointerFileSmudger)),
    Bytes(Vec<u8>),
}

/// Describes a single Readable Xet file
#[derive(Clone)]
pub struct XetRFileObject {
    pub content: FileContent,
}

impl XetRFileObject {
    /// Returns the length of the file
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        match &self.content {
            FileContent::Pointer((pfile, _)) => pfile.filesize() as usize,
            FileContent::Bytes(b) => b.len(),
        }
    }

    /// Reads the contents of a file returning (bytes, EOF)
    /// Note that offset/count may go past the end of the file and that
    /// in that case, all bytes till the end of file are returned.
    /// EOF is flagged the end of the file is reached by the read.
    pub async fn read(&self, offset: u64, count: u32) -> anyhow::Result<(Vec<u8>, bool)> {
        let mut start = offset as usize;
        let mut end = offset as usize + count as usize;
        let len = self.len();
        let eof = end >= len;
        if start >= len {
            start = len;
        }
        if end > len {
            end = len;
        }
        match &self.content {
            FileContent::Bytes(b) => Ok((b[start..end].to_vec(), eof)),
            FileContent::Pointer((_, translator)) => {
                let mut output: Vec<u8> = Vec::new();
                translator
                    .smudge_to_writer(&mut output, Some((start, end)))
                    .await?;
                Ok((output, eof))
            }
        }
    }

    /// Downloads the contents of a file and write them to disk
    /// at location specified by path.
    pub async fn get(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let mut writer = BufWriter::new(std::fs::File::create(path)?);

        match &self.content {
            FileContent::Bytes(b) => writer.write_all(&b)?,
            FileContent::Pointer((_, translator)) => {
                translator
                    .smudge_to_writer(&mut writer, Some((0, self.len())))
                    .await?;
            }
        }

        Ok(())
    }

    // Downloads the contents of a file and write them through a queue
    // to disk at location specified by path.
    // pub async fn get_through_queue(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
    //     match &self.content {
    //         FileContent::Bytes(_) => self.get(path),
    //         FileContent::Pointer((_, translator)) => {
    //             let (sender, receiver) = mpsc::channel::<Vec<u8>>(BLOB_WRITE_MPSC_CHANNEL_SIZE);
    //             let taskhandle = tokio::spawn(async move {
    //                 translator
    //             })
    //         }
    //     }
    // }
}
