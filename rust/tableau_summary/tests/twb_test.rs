use std::fs::File;
use std::io::Read;
use serde::{Deserialize, Serialize};
use tableau_summary::twb::diff::schema::TwbSummaryDiffContent;
use tableau_summary::twb::diff::util::DiffProducer;
use tableau_summary::twb::printer::summarize_twb_from_reader;
use tableau_summary::twb::raw::datasource::connection::Connection;
use tableau_summary::twb::TwbAnalyzer;
use tableau_summary::xml::XmlExt;
use crate::SummaryDiffData::Twb;

const BOOKSTORE: &str = "tests/data/federated.1i49ou20iq1y321232eee18hvwey.tds";
const SUPERSTORE_WB: &str = "tests/data/Superstore.twb";
const SUPERSTORE_WB2: &str = "tests/data/Superstore2.twb";
const SUPERSTORE_WB_NODASH: &str = "tests/data/SuperstoreNoDash.twb";

fn setup_logging() {
    tracing_subscriber::fmt::init();
}

#[test]
#[ignore = "need file"]
fn test_connection_parse() {
    setup_logging();
    let mut file = File::open(BOOKSTORE).unwrap();
    let mut s = String::new();
    let _ = file.read_to_string(&mut s).unwrap();
    let doc = roxmltree::Document::parse(&s).unwrap();
    let root = doc.root();
    let root = root.find_all_tagged_descendants("datasource")[0];
    let connection = root.get_tagged_child("connection").unwrap();
    let connection_info = Connection::from(connection);
    let s = serde_json::to_string(&connection_info).unwrap();
    println!("{s}");
}

#[test]
#[ignore = "need file"]
fn test_workbook_connection_parse() {
    setup_logging();
    let mut file = File::open(SUPERSTORE_WB).unwrap();
    let mut s = String::new();
    let _ = file.read_to_string(&mut s).unwrap();
    let doc = roxmltree::Document::parse(&s).unwrap();
    let root = doc.root();
    let root = root.find_all_tagged_descendants("workbook")[0];
    let sources = root.get_tagged_child("datasources").unwrap();
    let connections = sources.find_tagged_children("datasource")
        .into_iter()
        .flat_map(|d| d.get_tagged_child("connection"))
        .map(Connection::from)
        .collect::<Vec<_>>();
    let s = serde_json::to_string(&connections).unwrap();
    println!("{s}");
}


#[test]
#[ignore = "need file"]
fn test_parse_twb() {
    setup_logging();
    let mut a = TwbAnalyzer::new();
    let mut file = File::open(SUPERSTORE_WB).unwrap();
    let mut buf = Vec::new();
    let _ = file.read_to_end(&mut buf).unwrap();
    a.process_chunk(&buf);
    let summary = a.finalize().unwrap();
    assert!(summary.is_some());
    let s = serde_json::to_string(&summary.unwrap()).unwrap();
    println!("{s}");
}

#[test]
#[ignore = "need file"]
fn test_parse_datasource_model() {
    setup_logging();
    let mut file = File::open(SUPERSTORE_WB).unwrap();
    let summary = summarize_twb_from_reader(&mut file).unwrap().unwrap();
    let data = &summary.datasources;

    let s = serde_json::to_string(data).unwrap();
    println!("{s}");
    assert_eq!("Sales Commission", data[2].name);
}

#[test]
#[ignore = "need file"]
fn test_parse_twb_no_dash() {
    setup_logging();
    let mut a = TwbAnalyzer::new();
    let mut file = File::open(SUPERSTORE_WB_NODASH).unwrap();
    let mut buf = Vec::new();
    let _ = file.read_to_end(&mut buf).unwrap();
    a.process_chunk(&buf);
    let summary = a.finalize().unwrap();
    assert!(summary.is_some());
    let s = serde_json::to_string(&summary.unwrap()).unwrap();
    println!("{s}");
}

/// The resulting struct to be output by the command.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct DiffOutput {
    /// status of the diff calculation. See: [DiffError::get_code] for the code mapping
    pub status: u8,
    /// If there is an error, a desriptor of the issue.
    pub error_details: Option<String>,
    /// The diffs for each summary on the file.
    pub summaries: Vec<SummaryDiff>,
}

/// A diff for a particular summarization of a file.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct SummaryDiff {
    /// type of summary being diff'ed
    pub summary_type: String,
    /// whether a diff was found for the summary or not
    pub has_diff: bool,
    /// version of the summary-diff schema
    pub version: u8,
    /// The actual diff for the summary. If there is no diff, then this is None.
    pub summary_diff: Option<SummaryDiffData>,
}

/// An enum, whose values constitute the different types of summaries we can diff on.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
#[serde(untagged)]
pub enum SummaryDiffData {
    Twb(TwbSummaryDiffContent),
}


#[test]
#[ignore = "need file"]
fn test_diff_twb() {
    setup_logging();
    let mut a = TwbAnalyzer::new();
    let mut file = File::open(SUPERSTORE_WB).unwrap();
    let mut buf = Vec::new();
    let _ = file.read_to_end(&mut buf).unwrap();
    a.process_chunk(&buf);
    let before = a.finalize().unwrap().unwrap();
    let s = serde_json::to_string(&before).unwrap();
    println!("before: {s}");

    let mut a = TwbAnalyzer::new();
    let mut file = File::open(SUPERSTORE_WB2).unwrap();
    let mut buf = Vec::new();
    let _ = file.read_to_end(&mut buf).unwrap();
    a.process_chunk(&buf);
    let after = a.finalize().unwrap().unwrap();
    let s = serde_json::to_string(&after).unwrap();
    println!("after: {s}");

    let diff = TwbSummaryDiffContent::new_diff(&before, &after);
    let x = DiffOutput {
        status: 0,
        error_details: None,
        summaries: vec![SummaryDiff {
            summary_type: "twb".to_string(),
            has_diff: true,
            version: 0,
            summary_diff: Some(Twb(diff)),
        }],
    };
    let s = serde_json::to_string(&x).unwrap();
    println!("comp: {s}");
}

