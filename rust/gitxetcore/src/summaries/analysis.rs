use super::csv::{CSVAnalyzer, CSVSummary};
use crate::errors::Result;
use libmagic::libmagic::LibmagicSummary;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use tracing::{error, warn};
use tableau_summary::tds::{TdsAnalyzer, TdsSummary};
use tableau_summary::twb::{TwbAnalyzer, TwbSummary, TwbSummaryVersioner};

#[derive(Default)]
pub struct FileAnalyzers {
    pub csv: Option<CSVAnalyzer>,
    pub twb: Option<TwbAnalyzer>,
    pub tds: Option<TdsAnalyzer>,
}

lazy_static::lazy_static! {
    static ref CSV_WARNING_COUNTER: AtomicUsize = AtomicUsize::new(0);
}
const CSV_WARNING_THRESHOLD: usize = 3;
pub const ADDITIONAL_SUMMARY_VERSION: u32 = 0;

impl FileAnalyzers {
    fn process_chunk_impl(&mut self, chunk: &[u8]) -> Result<()> {
        if let Some(csv) = &mut self.csv {
            csv.process_chunk(chunk)?;
        }
        if let Some(twb) = &mut self.twb {
            twb.process_chunk(chunk);
        }
        if let Some(tds) = &mut self.tds {
            tds.process_chunk(chunk);
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
        let mut additional_summaries = SummaryExt::new();
        if let Some(twb) = &mut self.twb {
            additional_summaries.twb = twb.finalize()?;
        }
        if let Some(tds) = &mut self.tds {
            additional_summaries.tds = tds.finalize()?;
        }
        ret.additional_summaries = Some(additional_summaries);

        Ok(ret)
    }

    pub fn finalize(&mut self, file_path: &Path) -> Option<FileSummary> {
        let result = self.finalize_impl();

        if let Some(csv) = &mut self.csv {
            if let Some(warning) = csv.get_parse_warnings() {
                if !csv.silence_warnings {
                    let num_warnings = CSV_WARNING_COUNTER.load(SeqCst);
                    if num_warnings < CSV_WARNING_THRESHOLD {
                        warn!(
                            "Summaries for {:?} will not be available as parsing errors were detected: {}",
                            file_path, warning
                        );
                        let prev = CSV_WARNING_COUNTER.fetch_add(1, SeqCst);
                        if prev + 1 == CSV_WARNING_THRESHOLD {
                            // we have printed CSV_WARNING_THRESHOLD errors
                            warn!("Too many warnings. No more CSV warnings will be printed.");
                            warn!("You can run 'git xet config --local log.silentsummary true'");
                            warn!("or set the environment variable XET_LOG_SILENTSUMMARY=true to disable all parser warnings");
                        }
                    }
                }
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
    pub additional_summaries: Option<SummaryExt>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct SummaryExt {
    pub version: u32,
    // Tableau workbook summary
    pub twb: Option<TwbSummaryVersioner>,

    // Tableau datasource summary
    pub tds: Option<TdsSummary>,
}

impl FileSummary {
    pub fn merge_in(&mut self, other: Self, _key: &str) {
        if other.csv.is_some() {
            self.csv = other.csv;
        }
        if other.libmagic.is_some() {
            self.libmagic = other.libmagic;
        }
        if let Some(other_sum) = other.additional_summaries {
            let mut current = self.additional_summaries.take().unwrap_or_default();
            current.version = other_sum.version;
            if other_sum.twb.is_some() {
                current.twb = other_sum.twb;
            }
            if other_sum.tds.is_some() {
                current.tds = other_sum.tds;
            }
            self.additional_summaries = Some(current);
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

        if self.additional_summaries != other.additional_summaries {
            match (self.additional_summaries.as_ref(), other.additional_summaries.as_ref()) {
                (_, None) => ret.additional_summaries = None,
                (Some(a), Some(b)) => {
                    let mut ret_sum = SummaryExt::default();
                    if a.version != b.version {
                        ret_sum.version = b.version;
                    }
                    if a.twb != b.twb {
                        ret_sum.twb = b.twb.clone();
                    }
                    if a.tds != b.tds {
                        ret_sum.tds = b.tds.clone();
                    }
                    ret.additional_summaries = Some(ret_sum);
                },
                (None, x) => {
                    ret.additional_summaries = x.cloned();
                }
            }
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
        if let Some(additional_summaries) = self.additional_summaries.as_ref() {
            if additional_summaries.twb.is_some() {
                ret.push_str("twb;");
            }
            if additional_summaries.tds.is_some() {
                ret.push_str("tds;");
            }
        }
        ret
    }
}

impl SummaryExt {
    pub fn new() -> Self {
        Self {
            version: ADDITIONAL_SUMMARY_VERSION,
            ..Default::default()
        }
    }


}
