use crate::diff::csv::CsvSummaryDiffContent;
use crate::diff::error::DiffError;
use crate::summaries::summary_type::SummaryType;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use crate::diff::tds::TdsSummaryDiffContent;
use crate::diff::twb::TwbSummaryDiffContent;

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
    pub summary_type: SummaryType,
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
    Csv(CsvSummaryDiffContent),
    Twb(TwbSummaryDiffContent),
    Tds(TdsSummaryDiffContent),
}

impl From<DiffError> for DiffOutput {
    fn from(err: DiffError) -> Self {
        let message = format!("{err:?}");
        Self {
            status: err.get_code(),
            error_details: Some(message),
            summaries: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::output::SummaryDiffData::Csv;
    use crate::summaries::csv::CSVSummary;

    #[test]
    fn test_serde() {
        let csv_data = CsvSummaryDiffContent {
            before: Some(CSVSummary {
                headers: vec!["c1".to_string()],
                ..Default::default()
            }),
            ..Default::default()
        };

        let summaries = vec![SummaryDiff {
            summary_type: SummaryType::Csv,
            has_diff: true,
            version: 1,
            summary_diff: Some(Csv(csv_data)),
        }];

        let data = DiffOutput {
            status: 0,
            error_details: None,
            summaries,
        };

        let output = serde_json::to_string(&data).unwrap();
        println!("Data:\n{}", output);

        let deserialized: DiffOutput = serde_json::from_str(&output).unwrap();
        assert_eq!(deserialized, data);
    }
}
