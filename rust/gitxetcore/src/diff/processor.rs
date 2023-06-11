use crate::diff::error::DiffError;
use crate::diff::output::{SummaryDiff, SummaryDiffData};
use crate::summaries::analysis::FileSummary;
use crate::summaries::summary_type::SummaryType;

/// Trait that different types of summaries can implement to generate diffs.
///
/// The associated type `SummaryData` represents the type of summary data that is worked on.
/// Typically, this data is pulled from a [FileSummary] for a particular version of a file.
///
/// There are three methods to implement for getting diffs:
/// - [get_diff_impl] performs the diff across 2 summaries.
/// - [get_insert_diff] performs diff logic on an "added" or "new" summary.
/// - [get_remove_diff] performs diff logic on an "removed" summary, with the old contents provided.
///
/// The main entrypoint to this trait is through the [get_diff] method, which handles much of the
/// boilerplate logic around the different cases of summary availability we might have. For example,
/// determing whether the summaries for 2 files indicate an insertion, deletion, or no-op.
pub trait SummaryDiffProcessor {
    type SummaryData: PartialEq;

    /// Get the type of summary this struct corresponds to.
    fn get_type(&self) -> SummaryType;

    /// Get the version of the summary data struct.
    fn get_version(&self) -> u8;

    /// Extract a reference to the [SummaryData] from the provided [FileSummary].
    ///
    /// Note: we need to have the lifetime specifiers for this function since we are extracting
    /// a reference to the `summary` struct, so we need to ensure that that reference can live
    /// as long as the processor struct.
    fn get_data<'a>(&'a self, summary: &'a FileSummary) -> Option<&Self::SummaryData>;

    /// Get a diff for the summary as if it was for a newly inserted file.
    fn get_insert_diff(&self, summary: &Self::SummaryData) -> Result<SummaryDiffData, DiffError>;

    /// Get a diff for the summary as if it was for a deleted file.
    fn get_remove_diff(&self, summary: &Self::SummaryData) -> Result<SummaryDiffData, DiffError>;

    /// Perform the actual diff logic between the 2 summaries, where `before` is being changed into
    /// `after`. If there are no differences between the two summaries to show, then we expect:
    /// Err(DiffError::NoDiff) to be returned.
    fn get_diff_impl(
        &self,
        before: &Self::SummaryData,
        after: &Self::SummaryData,
    ) -> Result<SummaryDiffData, DiffError>;

    /// Gets the summary-diff for the two summaries: `before` and `after`. This mainly takes care of
    /// the boilerplate code of resolving the `Option` of the [FileSummary]s and whether they might
    /// contain the [SummaryData]. It then calls other methods in this trait to actually generate
    /// the diffs.
    fn get_diff(
        &self,
        before: Option<&FileSummary>,
        after: Option<&FileSummary>,
    ) -> Result<SummaryDiff, DiffError> {
        // extract the before and after data for this summary type from the file summaries.
        let before_data: Option<&Self::SummaryData> = if let Some(before_summary) = before {
            self.get_data(before_summary)
        } else {
            None
        };

        let after_data: Option<&Self::SummaryData> = if let Some(after_summary) = after {
            self.get_data(after_summary)
        } else {
            None
        };

        /*
        Cases (before(before_data), after(after_data)):
        (None, None)             => No Summaries
        (Some(None), None)       => No Summaries
        (Some(Some), None)       => Removal
        (None, Some(None))       => No Summaries
        (None, Some(Some))       => Insertion
        (Some(None), Some(None)) => No Summaries
        (Some(Some), Some(None)) => After summary not found
        (Some(None), Some(Some)) => Before summary not found
        (Some(Some), Some(Some)) => Diff impl
         */
        let result = match (before, before_data, after, after_data) {
            (None, _, None, _) => Err(DiffError::NoSummaries),
            (Some(_), None, None, _) => Err(DiffError::NoSummaries),
            (Some(_), Some(before_summary), None, _) => self.get_remove_diff(before_summary),
            (None, _, Some(_), None) => Err(DiffError::NoSummaries),
            (None, _, Some(_), Some(after_summary)) => self.get_insert_diff(after_summary),
            (Some(_), None, Some(_), None) => Err(DiffError::NoSummaries),
            (Some(_), Some(_), Some(_), None) => Err(DiffError::AfterSummaryNotFound),
            (Some(_), None, Some(_), Some(_)) => Err(DiffError::BeforeSummaryNotFound),
            (Some(_), Some(before_summary), Some(_), Some(after_summary)) => {
                if before_summary.eq(after_summary) {
                    Err(DiffError::NoDiff)
                } else {
                    self.get_diff_impl(before_summary, after_summary)
                }
            }
        };
        result
            .map(|diff_data| SummaryDiff {
                summary_type: self.get_type(),
                has_diff: true,
                version: self.get_version(),
                summary_diff: Some(diff_data),
            })
            .or_else(|err| match err {
                // If there is no diff, then that is ok, we can indicate in our output.
                DiffError::NoDiff => Ok(SummaryDiff {
                    summary_type: self.get_type(),
                    has_diff: false,
                    version: self.get_version(),
                    summary_diff: None,
                }),
                e => Err(e),
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering::SeqCst;

    use crate::diff::csv::CsvSummaryDiffContent;
    use crate::diff::error::DiffError::{
        AfterSummaryNotFound, BeforeSummaryNotFound, NoSummaries, Unknown,
    };
    use crate::diff::output::SummaryDiffData::Csv;
    use crate::summaries::csv::CSVSummary;

    use super::*;

    #[derive(Default)]
    struct TestProcessor {
        expected_summary: Option<SummaryDiffData>,
        remove_calls: AtomicU32,
        insert_calls: AtomicU32,
        diff_calls: AtomicU32,
        err_override: Option<DiffError>,
    }

    impl TestProcessor {
        fn get_error(&self) -> DiffError {
            self.err_override
                .clone()
                .unwrap_or_else(|| Unknown("failure in struct".to_string()))
        }
    }

    impl SummaryDiffProcessor for TestProcessor {
        type SummaryData = CSVSummary;

        fn get_type(&self) -> SummaryType {
            SummaryType::Csv
        }

        fn get_version(&self) -> u8 {
            1
        }

        fn get_data<'a>(&'a self, summary: &'a FileSummary) -> Option<&Self::SummaryData> {
            summary.csv.as_ref()
        }

        fn get_insert_diff(
            &self,
            _summary: &Self::SummaryData,
        ) -> Result<SummaryDiffData, DiffError> {
            self.insert_calls.fetch_add(1, SeqCst);
            self.expected_summary.clone().ok_or(self.get_error())
        }

        fn get_remove_diff(
            &self,
            _summary: &Self::SummaryData,
        ) -> Result<SummaryDiffData, DiffError> {
            self.remove_calls.fetch_add(1, SeqCst);
            self.expected_summary.clone().ok_or(self.get_error())
        }

        fn get_diff_impl(
            &self,
            _before: &Self::SummaryData,
            _after: &Self::SummaryData,
        ) -> Result<SummaryDiffData, DiffError> {
            self.diff_calls.fetch_add(1, SeqCst);
            self.expected_summary.clone().ok_or(self.get_error())
        }
    }

    fn get_test_csv_summary() -> CSVSummary {
        CSVSummary {
            headers: vec!["a".to_string()],
            ..Default::default()
        }
    }

    #[test]
    fn test_diff_cases() {
        let proc = TestProcessor {
            expected_summary: Some(Csv(CsvSummaryDiffContent {
                before: Some(get_test_csv_summary()),
                ..Default::default()
            })),
            ..Default::default()
        };
        let file_none = FileSummary::default();
        let mut file_some = FileSummary::default();
        file_some.csv = Some(CSVSummary::default());
        let mut file_some_diff = FileSummary::default();
        let summary = get_test_csv_summary();
        file_some_diff.csv = Some(summary);

        /*
        Cases (before(before_data), after(after_data)):
        (None, None)             => No Summaries
        (Some(None), None)       => No Summaries
        (Some(Some), None)       => Removal
        (None, Some(None))       => No Summaries
        (None, Some(Some))       => Insertion
        (Some(None), Some(None)) => No Summaries
        (Some(Some), Some(None)) => After summary not found
        (Some(None), Some(Some)) => Before summary not found
        (Some(Some), Some(Some)) => Diff impl
         */

        // (None, None)             => No Summaries
        let err = proc.get_diff(None, None).expect_err("");
        assert_eq!(err.get_code(), NoSummaries.get_code());

        // (Some(None), None)       => No Summaries
        let err = proc.get_diff(Some(&file_none), None).expect_err("");
        assert_eq!(err.get_code(), NoSummaries.get_code());

        // (Some(Some), None)       => Removal
        let diff = proc.get_diff(Some(&file_some), None).unwrap();
        assert!(diff.has_diff);
        assert_eq!(diff.version, 1);
        assert_eq!(diff.summary_type, SummaryType::Csv);
        assert_eq!(
            diff.summary_diff.unwrap(),
            proc.expected_summary.clone().unwrap()
        );
        assert_eq!(proc.remove_calls.load(SeqCst), 1);

        // (None, Some(None))       => No Summaries
        let err = proc.get_diff(None, Some(&file_none)).expect_err("");
        assert_eq!(err.get_code(), NoSummaries.get_code());

        // (None, Some(Some))       => Insertion
        let diff = proc.get_diff(None, Some(&file_some)).unwrap();
        assert!(diff.has_diff);
        assert_eq!(diff.version, 1);
        assert_eq!(diff.summary_type, SummaryType::Csv);
        assert_eq!(
            diff.summary_diff.unwrap(),
            proc.expected_summary.clone().unwrap()
        );
        assert_eq!(proc.insert_calls.load(SeqCst), 1);

        // (Some(None), Some(None)) => No Summaries
        let err = proc
            .get_diff(Some(&file_none), Some(&file_none))
            .expect_err("");
        assert_eq!(err.get_code(), NoSummaries.get_code());

        // (Some(Some), Some(None)) => After summary not found
        let err = proc
            .get_diff(Some(&file_some), Some(&file_none))
            .expect_err("");
        assert_eq!(err.get_code(), AfterSummaryNotFound.get_code());

        // (Some(None), Some(Some)) => Before summary not found
        let err = proc
            .get_diff(Some(&file_none), Some(&file_some))
            .expect_err("");
        assert_eq!(err.get_code(), BeforeSummaryNotFound.get_code());

        // (Some(Some), Some(Some)) => Diff impl
        let diff = proc
            .get_diff(Some(&file_some), Some(&file_some_diff))
            .unwrap();
        assert!(diff.has_diff);
        assert_eq!(diff.version, 1);
        assert_eq!(diff.summary_type, SummaryType::Csv);
        assert_eq!(
            diff.summary_diff.unwrap(),
            proc.expected_summary.clone().unwrap()
        );
        assert_eq!(proc.diff_calls.load(SeqCst), 1);
    }

    #[test]
    fn test_diff_eq_fast_return() {
        let proc = TestProcessor {
            expected_summary: Some(Csv(CsvSummaryDiffContent {
                before: Some(get_test_csv_summary()),
                ..Default::default()
            })),
            ..Default::default()
        };
        let mut file_some = FileSummary::default();
        file_some.csv = Some(CSVSummary::default());

        let diff = proc.get_diff(Some(&file_some), Some(&file_some)).unwrap();
        assert!(!diff.has_diff);
        assert_eq!(diff.version, 1);
        assert_eq!(diff.summary_type, SummaryType::Csv);
        assert!(diff.summary_diff.is_none());
        assert_eq!(proc.diff_calls.load(SeqCst), 0);
    }

    #[test]
    fn test_diff_error() {
        let proc = TestProcessor {
            expected_summary: None,
            ..Default::default()
        };
        let mut file_some = FileSummary::default();
        file_some.csv = Some(CSVSummary::default());

        let err = proc.get_diff(None, Some(&file_some)).expect_err("");
        assert_eq!(err.get_code(), proc.get_error().get_code());
        assert_eq!(proc.insert_calls.load(SeqCst), 1);
    }

    #[test]
    fn test_no_diff() {
        let proc = TestProcessor {
            expected_summary: None,
            err_override: Some(DiffError::NoDiff),
            ..Default::default()
        };
        let mut file_some = FileSummary::default();
        file_some.csv = Some(CSVSummary::default());
        let mut file_some_other = FileSummary::default();
        let summary = get_test_csv_summary();
        file_some_other.csv = Some(summary);

        let diff = proc
            .get_diff(Some(&file_some), Some(&file_some_other))
            .unwrap();
        assert!(!diff.has_diff);
        assert_eq!(diff.version, 1);
        assert_eq!(diff.summary_type, SummaryType::Csv);
        assert!(diff.summary_diff.is_none());
        assert_eq!(proc.diff_calls.load(SeqCst), 1);
    }
}
