use crate::tds::{TdsAnalyzer, TdsSummaryVersioner};
use std::fs::File;
use std::io::Read;
use std::path::Path;

const CHUNK_SIZE: usize = 65536;

// Reads the whole file from disk, and prints the Tds analysis.
// Intended to be used for small passthrough (non-pointer) files.
pub fn print_tds_summary_from_reader(file: &mut impl Read) -> anyhow::Result<()> {
    let result = summarize_tds_from_reader(file)?;
    let json = serde_json::to_string_pretty(&result)?;
    println!("{json}");
    Ok(())
}

// Reads the whole file from disk, and returns the Tds analysis.
// Intended to be used for small passthrough (non-pointer) files.
pub fn summarize_tds_from_reader(
    file: &mut impl Read,
) -> anyhow::Result<Option<TdsSummaryVersioner>> {
    let mut analyzer = TdsAnalyzer::default();

    let mut chunk: Vec<u8> = vec![0; CHUNK_SIZE];

    loop {
        let n = file.read(&mut chunk[..])?;
        if n == 0 {
            break;
        }
        analyzer.process_chunk(&chunk[..n]);
    }

    let result = analyzer.finalize()?;
    Ok(result)
}

// Reads the whole file from disk, and prints the Tds analysis.
// Intended to be used for small passthrough (non-pointer) files.
pub fn print_tds_summary(file_path: &Path) -> anyhow::Result<()> {
    let mut file = File::open(file_path)?;
    print_tds_summary_from_reader(&mut file)
}
