use std::mem;

use anyhow::anyhow;
use roxmltree::Node;
use serde::{Deserialize, Serialize};
use crate::twb::raw::dashboard::{DashboardMeta, parse_dashboards};


use crate::twb::raw::datasource::{Datasource, parse_datasources};
use crate::twb::raw::worksheet::{parse_worksheets, WorksheetMeta};
use crate::twb::summary::datasource::WorkbookDatasource;
use crate::xml::XmlExt;

pub mod raw;
pub mod printer;
pub mod summary;

const PARSER_VERSION: u32 = 1;
const VERSION_KEY: &str = "version";
const CAPTION_KEY: &str = "caption";
const NAME_KEY: &str = "name";

/// Analyzes Tableau's workbook files to produce a summary
/// that can visualize the key pieces of a workbook.
#[derive(Default, Debug)]
pub struct TwbAnalyzer {
    /// for now, just store all the XML content. This may cause memory issues
    /// if the workbook file is large, in which case, we can build a streaming
    /// reader to collect the info we want.
    content_buffer: Vec<u8>,
}

/// A summary of a Tableau Workbook File (*.twb) providing the
/// key components of a workbook.
///
/// repository-location indicates the views of the workbook
#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TwbSummary {
    pub parse_version: u32,
    pub wb_version: String,
    pub datasources: Vec<WorkbookDatasource>,
    pub worksheets: Vec<WorksheetMeta>,
    pub dashboards: Vec<DashboardMeta>,
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
pub struct TwbRaw {
    wb_version: String,
    datasources: Vec<Datasource>,
    worksheets: Vec<WorksheetMeta>,
    dashboards: Vec<DashboardMeta>,
}

impl TwbAnalyzer {
    pub fn new() -> Self {
        Self {
            content_buffer: vec![],
        }
    }

    pub fn process_chunk(&mut self, chunk: &[u8]) {
        self.content_buffer.extend_from_slice(chunk);
    }

    pub fn finalize(&mut self) -> anyhow::Result<Option<TwbSummary>> {
        let mut buf = vec![];
        mem::swap(&mut self.content_buffer, &mut buf);

        // Parse XML into XML tree
        let content_string = String::from_utf8(buf)
            .map_err(|e| anyhow!("parsed TWB is not UTF-8: {e:?}"))?;
        let document = roxmltree::Document::parse(&content_string)
            .map_err(|e| anyhow!("TWB content wasn't parsed as XML: {e:?}"))?;
        let root = document.root().get_tagged_child("workbook")
            .ok_or(anyhow!("no workbook node"))?;

        // Build raw workbook model
        let raw_workbook = TwbRaw::try_from(root)?;

        // Summarize from the raw workbook model
        let datasources = raw_workbook.datasources.iter()
            .map(WorkbookDatasource::from)
            .collect();
        let worksheets = raw_workbook.worksheets;
        let dashboards = raw_workbook.dashboards;
        Ok(Some(TwbSummary {
            parse_version: PARSER_VERSION,
            wb_version: raw_workbook.wb_version,
            datasources,
            worksheets,
            dashboards,
        }))
    }
}

impl<'a, 'b> TryFrom<Node<'a, 'b>> for TwbRaw {
    type Error = anyhow::Error;

    fn try_from(n: Node) -> anyhow::Result<Self> {
        if n.get_tag() != "workbook" {
            return Err(anyhow!("trying to convert a ({}) to a top-level workbook", n.get_tag()));
        }
        let datasources = n.get_tagged_child("datasources")
            .ok_or(anyhow!("no datasources for workbook"))
            .and_then(parse_datasources)?;
        let worksheets = n.get_tagged_child("worksheets")
            .ok_or(anyhow!("no worksheets for workbook"))
            .and_then(parse_worksheets)?;
        let dashboards = n.get_tagged_child("dashboards")
            .ok_or(anyhow!("no dashboards for workbook"))
            .and_then(parse_dashboards)?;

        Ok(Self {
            wb_version: n.get_attr(VERSION_KEY),
            datasources,
            worksheets,
            dashboards,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Read;


}
