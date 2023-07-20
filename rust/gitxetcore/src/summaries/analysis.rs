use super::csv::{CSVAnalyzer, CSVSummary};
use crate::errors::Result;
use libmagic::libmagic::LibmagicSummary;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::{error, warn};

#[derive(Default)]
pub struct FileAnalyzers {
    pub csv: Option<CSVAnalyzer>,
}

impl FileAnalyzers {
    fn process_chunk_impl(&mut self, chunk: &[u8]) -> Result<()> {
        if let Some(csv) = &mut self.csv {
            csv.process_chunk(chunk)?;
        }
        Ok(())
    }

    pub fn process_chunk(&mut self, chunk: &[u8], file_path: &Path, chunk_offset: usize) {
        let result = self.process_chunk_impl(chunk);

        match result {
            Ok(_) => (),
            Err(e) => {
                error!(
                    "Error occurred processing chunk [{:?}, {:?}) from {:?}, range : {:?}",
                    &chunk_offset,
                    (chunk_offset + chunk.len()),
                    &file_path,
                    &e
                );
            }
        };
    }

    fn finalize_impl(&mut self) -> Result<FileSummary> {
        let mut ret = FileSummary::default();
        if let Some(csv) = &mut self.csv {
            ret.csv = csv.finalize()?;
        }
        Ok(ret)
    }

    pub fn finalize(&mut self, file_path: &Path) -> Option<FileSummary> {
        let result = self.finalize_impl();

        if let Some(csv) = &mut self.csv {
            if let Some(warning) = csv.get_parse_warnings() {
                warn!("CSV parse error in {:?}: {}", file_path, warning);
            }
        }

        match result {
            Ok(summary) => Some(summary),
            Err(e) => {
                error!(
                    "Error occurred finalizing chunking of {:?}: {:?}",
                    &file_path, &e
                );
                None
            }
        }
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct FileSummary {
    pub csv: Option<CSVSummary>,

    // for historical reasons this is called libmagic but does not use libmagic
    pub libmagic: Option<LibmagicSummary>, 

    // A buffer to allow us to add more to the serialized options
    _buffer: Option<()>,
}

impl FileSummary {
    pub fn merge_in(&mut self, other: Self, _key: &str) {
        if other.csv.is_some() {
            self.csv = other.csv;
        }
        if other.libmagic.is_some() {
            self.libmagic = other.libmagic;
        }
    }

    pub fn diff(&self, other: &Self) -> Option<Self> {
        if self == other {
            return None;
        }
        let mut ret = Self::default();
        if self.csv != other.csv {
            ret.csv = other.csv.clone();
        }
        if self.libmagic != other.libmagic {
            ret.libmagic = other.libmagic.clone();
        }
        Some(ret)
    }

    pub fn list_types(&self) -> String {
        let mut ret = String::new();
        if self.csv.is_some() {
            ret.push_str("csv;");
        }
        if self.libmagic.is_some() {
            ret.push_str("libmagic;");
        }
        ret
    }
}
