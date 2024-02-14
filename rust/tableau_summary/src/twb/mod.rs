use serde::{Deserialize, Serialize};
use std::mem;
use anyhow::anyhow;
use dashboard::DashboardMeta;
use worksheet::WorksheetMeta;
use crate::twb::data_source::DataSourceMeta;

pub mod data_source;
pub mod worksheet;
pub mod dashboard;
mod xml;

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
    parse_version: u32,
    wb_version: String,
    datasources: Vec<DataSourceMeta>,
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
        let content_string = String::from_utf8(buf)
            .map_err(|e| anyhow!("parsed TWB is not UTF-8: {e:?}"))?;
        let document = roxmltree::Document::parse(&content_string)
            .map_err(|e| anyhow!("TWB content wasn't parsed as XML: {e:?}"))?;
        let root = xml::find_single_tagged_node(document.root(), "workbook")
            .ok_or(anyhow!("no workbook node"))?;
        let mut summary = TwbSummary {
            parse_version: PARSER_VERSION,
            wb_version: xml::get_attr(root, VERSION_KEY),
            ..Default::default()
        };

        for node in root.children() {
            match node.tag_name().name() {
                "datasources" => {
                    let datasources = data_source::parse_datasources(node)?;
                    summary.datasources = datasources;
                }
                "worksheets" => {
                    let worksheets = worksheet::parse_worksheets(node)?;
                    summary.worksheets = worksheets;
                }
                _ => {}
            }
        }
        Ok(Some(summary))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Read;

    use crate::twb::TwbAnalyzer;
    use crate::twb::xml::get_nodes_with_tags;

    #[test]
    fn test_parse_twb() {
        let mut a = TwbAnalyzer::new();
        let mut file = File::open("src/Superstore.twb").unwrap();
        let mut buf = Vec::new();
        let _ = file.read_to_end(&mut buf).unwrap();
        a.process_chunk(&buf);
        let summary = a.finalize().unwrap();
        assert!(summary.is_some());
    }

    #[test]
    fn test_get_nodes_with_tag() {
        let a = TwbAnalyzer::new();
        let mut file = File::open("src/Superstore.twb").unwrap();
        let mut s = String::new();
        let _ = file.read_to_string(&mut s).unwrap();
        let doc = roxmltree::Document::parse(&s).unwrap();
        let root = doc.root();
        let vec = get_nodes_with_tags(root, "datasources");
        let num_elts = vec.len();
        assert_eq!(num_elts, 29);
        let datasources = vec[0];
        let vec = get_nodes_with_tags(datasources, "datasource");
        let num_elts = vec.len();
        assert_eq!(num_elts, 4);
    }
}
