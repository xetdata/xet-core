use std::{
    io::{BufWriter, Write},
    path::Path,
    sync::Arc,
};

use progress_reporting::{DataProgressReporter, ReportedWriter};

use crate::data::*;

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
    pub async fn read_to_path(
        &self,
        path: impl AsRef<Path>,
        progress_reporter: Option<Arc<DataProgressReporter>>,
    ) -> anyhow::Result<()> {
        let mut writer = ReportedWriter::new(
            BufWriter::new(std::fs::File::create(path)?),
            &progress_reporter,
        );

        match &self.content {
            FileContent::Bytes(b) => writer.write_all(b)?,
            FileContent::Pointer((_, translator)) => {
                translator.smudge_to_writer(&mut writer, None).await?
            }
        }

        if let Some(pr) = progress_reporter.as_ref() {
            pr.register_progress(Some(1), None);
        }

        Ok(())
    }
}
