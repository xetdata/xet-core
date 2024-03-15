use std::fs::File;
use std::io::Read;
use tableau_summary::twb::printer::summarize_twb_from_reader;
use tableau_summary::twb::raw::datasource::connection::Connection;
use tableau_summary::twb::TwbAnalyzer;
use tableau_summary::xml::XmlExt;

const BOOKSTORE: &str = "tests/data/federated.1i49ou20iq1y321232eee18hvwey.tds";
const SUPERSTORE_WB: &str = "tests/data/Superstore.twb";

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
