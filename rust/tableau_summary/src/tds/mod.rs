use std::borrow::Cow;
use std::mem;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::twb::raw::datasource::RawDatasource;
use crate::twb::summary::datasource::Datasource;
use crate::xml::XmlExt;

pub mod printer;
pub mod diff;

const PARSER_VERSION: u32 = 1;

/// Analyzes Tableau's datasource files to produce a summary
/// that can visualize the schema of the datasource.
#[derive(Default, Debug)]
pub struct TdsAnalyzer {
    /// for now, just store all the XML content. This may cause memory issues
    /// if the datasource file is large, in which case, we can build a streaming
    /// reader to collect the info we want.
    content_buffer: Vec<u8>,
}


#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
#[repr(u32)]
pub enum TdsSummaryVersioner {
    #[default]
    Default = 0x00,
    V1(TdsSummaryV1) = PARSER_VERSION,
    // V2(TdsSummaryV2) = 0x02,
}

pub type TdsSummary = TdsSummaryV1;

impl TdsSummary {
    pub fn from_ref(summary: &TdsSummaryVersioner) -> Option<Cow<Self>> {
        match summary {
            TdsSummaryVersioner::Default => None,
            TdsSummaryVersioner::V1(s) => Some(Cow::Borrowed(s)),
        }
    }
}


/// A summary of a Tableau Datasource File (*.tds) providing the
/// schema.
#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TdsSummaryV1 {
    pub parse_version: u32,
    /// TODO: at some point, we may want to have a more detailed view of the datasource
    ///       (e.g. physical table relationships)
    pub datasource: Datasource,
}

impl TdsAnalyzer {
    pub fn new() -> Self {
        Self {
            content_buffer: vec![],
        }
    }

    pub fn process_chunk(&mut self, chunk: &[u8]) {
        self.content_buffer.extend_from_slice(chunk);
    }

    pub fn finalize(&mut self) -> anyhow::Result<Option<TdsSummaryVersioner>> {
        let mut buf = vec![];
        mem::swap(&mut self.content_buffer, &mut buf);

        // Parse XML into XML tree
        let content_string = String::from_utf8(buf)
            .map_err(|e| anyhow!("parsed TDS is not UTF-8: {e:?}"))?;
        let document = roxmltree::Document::parse(&content_string)
            .map_err(|e| anyhow!("TDS content wasn't parsed as XML: {e:?}"))?;
        let root = document.root().get_tagged_child("datasource")
            .ok_or(anyhow!("no datasource node"))?;

        // Build raw datasource model
        let raw_datasource = RawDatasource::from(root);
        let datasource = Datasource::from(&raw_datasource);

        // Summarize from the raw datasource model
        Ok(Some(TdsSummaryVersioner::V1(TdsSummary {
            parse_version: PARSER_VERSION,
            datasource,
        })))
    }
}
