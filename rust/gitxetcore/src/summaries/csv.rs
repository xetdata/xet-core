use anyhow::anyhow;
use clap::ArgEnum;
use std::ffi::OsStr;
use std::str::FromStr;
use std::{borrow::Cow, fs::File, io::Read, mem::take, path::Path};

use super::constants::*;
use crate::errors::Result;
use csv_core::{self, ReadRecordResult};
use data_analysis::analyzer_trait::Analyzer;
use data_analysis::histogram_float::{FloatHistogram, FloatHistogramSummary};
use data_analysis::sketches::{SpaceSavingSketch, SpaceSavingSketchSummary};
use more_asserts::*;
use serde::{Deserialize, Serialize};

#[derive(ArgEnum, Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CsvDelimiter {
    #[default]
    Comma,
    Tab,
}

impl FromStr for CsvDelimiter {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "comma" | "," => Ok(CsvDelimiter::Comma),
            "tab" | "\t" => Ok(CsvDelimiter::Tab),
            _ => Err(anyhow!("unrecognized csv delimiter str")),
        }
    }
}

impl From<CsvDelimiter> for u8 {
    fn from(value: CsvDelimiter) -> Self {
        match value {
            CsvDelimiter::Comma => b',',
            CsvDelimiter::Tab => b'\t',
        }
    }
}

#[derive(Default)]
pub struct ColumnContentAnalyzer {
    numeric_tracker: FloatHistogram,
    string_tracker: SpaceSavingSketch,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct ColumnContentSummary {
    numeric_summary: Option<FloatHistogramSummary>,
    string_summary: Option<SpaceSavingSketchSummary>,

    // A buffer to allow us to add more to the serialized options
    _buffer: Option<()>,
}

impl ColumnContentAnalyzer {
    pub fn add_str(&mut self, s: &str) -> Result<()> {
        if let Ok(n) = s.parse::<f64>() {
            self.numeric_tracker.add(n);
        }
        self.string_tracker.add(s);

        Ok(())
    }

    pub fn finalize(&mut self) -> Result<ColumnContentSummary> {
        Ok(ColumnContentSummary {
            numeric_summary: self.numeric_tracker.summary(),
            string_summary: self.string_tracker.summary(),
            _buffer: None,
        })
    }
}

#[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
/**
 * The results of the CSV Analyzer.
 */
pub struct CSVSummary {
    /// The headers of the CSV file.  If these are empty, then we assume that the first full
    /// line of input has not been parsed yet.  
    ///
    /// The headers are taken from the first row if all are nonempty, non-numeric strings, and
    /// set to Column 0, Column 1, etc.
    pub headers: Vec<String>,

    /// This is allowed to be empty, in which case no analyzers have been set yet.
    pub summaries: Vec<ColumnContentSummary>,
}

/** The CSV Analyzer.
 */
pub struct CSVAnalyzer {
    /// The headers of the CSV file.  If these are empty, then we assume that the first full
    /// line of input has not been parsed yet.  
    ///
    /// The headers are taken from the first row if all are nonempty, non-numeric strings, and
    /// set to Column 0, Column 1, etc.
    headers: Vec<String>,

    /// This is allowed to be empty, in which case no analyzers have been set yet.  
    analyzers: Vec<ColumnContentAnalyzer>,

    /// The number of columns.  May be zero when nothing is parsed yet.
    num_columns: usize,

    /// The csv parser part.
    csv_parser: csv_core::Reader,

    /// Buffers used; these are recycled chunk-to-chunk to avoid reallocations.
    output_buffer: Vec<u8>,

    ends_buffer: Vec<usize>,

    /// Annoyingly, the csv parser is in an intermediate state if we partially parse a record.
    /// It holds a current state, and, if the reason for stopping is that we run out of input,
    /// then it assumes that we hold all the state as is until the next input is fed in and
    /// the parsed fields get appended to the previous results (which we need to store).  
    ///
    /// These hold the points at which the output has successfully been written to our buffers and
    /// subsequent parses should resume.
    current_output_write_index: usize,
    current_ends_write_index: usize,

    /// In order to cross chunk boundaries, which may not correspond to the newlines.
    /// This holds data that has not yet been processed from the previous chunks.
    previous_leftover: Vec<u8>,

    /// If set there was a resumable parse error
    parse_warning: Option<(usize, String)>,

    /// If set, no warnings will ever be printed
    pub silence_warnings: bool,

    /// The total number of bytes analyzed
    total_bytes: usize,
}

impl CSVAnalyzer {
    pub fn process_chunk(&mut self, chunk: &[u8]) -> Result<()> {
        self.total_bytes += chunk.len();
        self.process_chunk_impl(chunk)
    }

    pub fn finalize(&mut self) -> Result<Option<CSVSummary>> {
        let size_threshold_min: usize = std::env::var_os(CSV_SUMMARY_SIZE_THRESHOLD_MIN_ENV_VAR)
            .as_ref()
            .and_then(|osstr| osstr.to_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(CSV_SUMMARY_SIZE_THRESHOLD_MIN);

        if self.total_bytes < size_threshold_min {
            Ok(None)
        } else {
            let mut ret = CSVSummary::default();
            let summaries = self.finalize_impl()?;
            ret.headers = self.headers.clone();
            ret.summaries = summaries;
            Ok(Some(ret))
        }
    }
    pub fn new(silence_warnings: bool, delimiter: u8) -> Self {
        let mut builder = csv_core::ReaderBuilder::new();
        builder.delimiter(delimiter);
        Self {
            headers: Vec::new(),
            analyzers: Vec::new(),
            num_columns: 0,
            csv_parser: builder.build(),
            output_buffer: vec![0; 4096],
            ends_buffer: vec![0; 256],
            current_output_write_index: 0,
            current_ends_write_index: 0,
            previous_leftover: Vec::with_capacity(1024),
            parse_warning: None,
            silence_warnings,
            total_bytes: 0,
        }
    }
}

impl CSVAnalyzer {
    fn process_chunk_impl(&mut self, chunk: &[u8]) -> Result<()> {
        let mut pos: usize = 0;

        // We need to do this manually here, as the first and last ones need to be treated specially.
        if !self.previous_leftover.is_empty() {
            let parse_base_size = self.previous_leftover.len();

            let mut pull_pos: usize = 0;
            let mut leftover_pull_start: usize = 0;

            // The previous_leftover buffer has data only if there was parse with insufficient input data earlier.
            // Therefore, copy in new data to this buffer  If it is unsuccessful due to limited input, copy more of the input chunk into the buffer.
            // If the input chunk has been entirely copied into the buffer, then just exit -- we'll have
            // to wait for more chunks.
            loop {
                // We don't need much; this mainly comes up when we're avoiding the UTF8 boundary issue.
                // However, it is within the spec of the csv parser to not finish the last part of an input
                // text (maybe due to mismatched quotes or something?) so we need to pull enough that we'll quickly
                // allow it to finish a parse.
                const ADD_SIZE: usize = 32;

                // Copy a chunk of data from the new chunk into previous leftover.
                let end_loc = usize::min(pull_pos + ADD_SIZE, chunk.len());

                // If there is no data to modify the previous_leftover chunk, then we need to wait for the next
                // chunk to come along to make this work.
                if pull_pos == end_loc {
                    return Ok(());
                }

                self.previous_leftover
                    .extend_from_slice(&chunk[pull_pos..end_loc]);

                pull_pos = end_loc;

                // Try a parse.  If it's successful, then clear out the previous buffer, set pos to the point
                // in the new data that would be at the end of the current parse. Then
                // continue to the main processing part of the loop.

                // Do some moving around to make the borrow checker happy
                loop {
                    let previous_leftover_tmp = take(&mut self.previous_leftover);
                    let (_full_parse, parse_advance) = self.process_next_record(
                        &previous_leftover_tmp[leftover_pull_start..],
                        false,
                    )?;
                    self.previous_leftover = previous_leftover_tmp; // So we don't lose allocations.

                    // This means that the complete parse actually consumed all of the content of the buffer,
                    // And we can go on to parsing directly from the chunk.
                    if parse_advance + leftover_pull_start >= parse_base_size {
                        pos = parse_advance + leftover_pull_start - parse_base_size;
                        self.previous_leftover.clear();
                        break;
                    } else {
                        // We need to loop back around until we've parsed all the parts that need
                        // the previous leftover buffer instead of the next chunk.
                        leftover_pull_start += parse_advance;
                        continue;
                    }
                }

                // If we've processed all of the bytes in the previous leftover buffer, we can
                // go to the next stage.
                if self.previous_leftover.is_empty() {
                    break;
                }
            }
        }

        debug_assert!(self.previous_leftover.is_empty());

        loop {
            // Pass a slice of the current buffer to the parser.
            let (full_parse, parse_advance) = self.process_next_record(&chunk[pos..], false)?;

            if full_parse {
                pos += parse_advance;
                continue;
            } else {
                // We're at the end.  Copy the remaining bit of the chunk to the previously allocated buffer if
                // needed, otherwise just return.
                if pos + parse_advance < chunk.len() {
                    debug_assert!(self.previous_leftover.is_empty());
                    self.previous_leftover
                        .extend_from_slice(&chunk[(pos + parse_advance)..]);
                    break;
                } else {
                    debug_assert_eq!(pos + parse_advance, chunk.len());
                }
                break;
            }
        }

        Ok(())
    }

    /** Process the next block of data.
     *  
     *  Returns a flag indicating a full parse, and the number of bytes of the input processed.
     *
     */
    fn process_next_record(&mut self, input: &[u8], is_final: bool) -> Result<(bool, usize)> {
        // Attempt a read of the current record.  Use the cached buffers here.

        // For some reason, this case screws up the internal state of the csv parser, making
        // it think it's done parsing the records if there are previously tracked states.
        // It's easiest to just skip it.
        if !is_final && input.is_empty() {
            return Ok((false, 0));
        }

        let mut read_record_wrap = |input: &[u8], out_buf: &mut [u8], ends_buf: &mut [usize]| {
            let (result, nb_input, nb_out, n_ends) =
                self.csv_parser.read_record(input, out_buf, ends_buf);

            // Intercept the results here to print.

            /* UNCOMMENT FOR DEBUGGING
            let input_s = if let Ok(s) = std::str::from_utf8(input) {
              s
            } else { "UTF8 Error" };

            eprintln!(
                "Parsing result ({:?}) : parsed {:?} bytes of {:?} into {:?} records: {:?}, raw_out: {:?} ({:?} bytes)",
                &result,
                nb_input,
                &_input_s,
                &n_ends,
                (0..n_ends)
                    .map(|i| {
                        let start = {
                            if i == 0 {
                                0
                            } else {
                                ends_buf[i - 1]
                            }
                        };
                        let end = ends_buf[i];
                        if let Ok(s) = std::str::from_utf8(&out_buf[start..end]) {
                          s
                        } else { "UTF8 Error" }
                    })
                    .collect::<Vec<&str>>()
                    .join("|"),
                    std::str::from_utf8(&out_buf[..nb_out]).unwrap(),
                    nb_out
            );
            */

            (result, nb_input, nb_out, n_ends)
        };

        // First, do the expected pass and parse things.  If this fails, then we need to
        // go into more complicated logic.
        let (mut result, mut current_input_read_index, n_bytes_output, n_positions_end) =
            read_record_wrap(
                input,
                &mut self.output_buffer[self.current_output_write_index..],
                &mut self.ends_buffer[self.current_ends_write_index..],
            );

        // For the next read, this is where we'll start.
        self.current_output_write_index += n_bytes_output;
        self.current_ends_write_index += n_positions_end;

        /* UNCOMMENT FOR DEBUGGING

        eprintln!(
            "  -> outbuffer = {:?}",
            std::str::from_utf8(&self.output_buffer[..self.current_output_write_index]).unwrap()
        );
        eprintln!(
            "  -> ends = {:?}",
            &self.ends_buffer[..self.current_ends_write_index]
        );
        */

        if result != ReadRecordResult::Record {
            loop {
                match result {
                    ReadRecordResult::InputEmpty => {
                        // If it's actually the final parse, we should now just go through and assume we've hit EOF, and that
                        // we're (hopefully) in a good place.
                        if is_final {
                            break;
                        } else {
                            // Record the current progress -- where we'll pick up next time.
                            return Ok((false, current_input_read_index));
                        }
                    }
                    ReadRecordResult::OutputFull => {
                        // DOC: The caller provided input was exhausted before the end of a record was found.

                        // Our output buffer was too small to hold the record,
                        // so increase it and try again.
                        self.output_buffer
                            .resize((5 * self.output_buffer.len()) / 4 + 512, 0);
                    }
                    ReadRecordResult::OutputEndsFull => {
                        // DOC: The caller provided output buffer of field end poisitions was filled before
                        // the next field could be parsed.  For this, however,

                        // Our ends buffer was too small to hold the record,
                        // so increase it and try again.
                        self.ends_buffer
                            .resize((5 * self.ends_buffer.len()) / 4 + 128, 0);
                    }
                    ReadRecordResult::Record => {
                        // Okay, successful read, we've done it.
                        break;
                    }
                    ReadRecordResult::End => {
                        // DOC: This only happens when an empty input string is passed to the parser.
                        // This can happen for the final newline, etc.  It is counted as a full parse.
                        if is_final {
                            break;
                        } else {
                            return Ok((false, 0));
                        }
                    }
                }

                // Retry the parse from the current position, filling more of the buffer.  This point
                // is only really
                let (
                    retry_result,
                    retry_n_bytes_input,
                    retry_n_bytes_output,
                    retry_n_positions_end,
                ) = read_record_wrap(
                    &input[current_input_read_index..],
                    &mut self.output_buffer[self.current_output_write_index..],
                    &mut self.ends_buffer[self.current_ends_write_index..],
                );

                result = retry_result;
                current_input_read_index += retry_n_bytes_input;
                self.current_output_write_index += retry_n_bytes_output;
                self.current_ends_write_index += retry_n_positions_end;

                /* UNCOMMENT FOR DEBUG
                eprintln!(
                    "  -> outbuffer = {:?}",
                    std::str::from_utf8(&self.output_buffer[..self.current_output_write_index])
                        .unwrap()
                );
                eprintln!(
                    "  -> ends = {:?}",
                    &self.ends_buffer[..self.current_ends_write_index]
                );
                */

                // Loop back to interpret the result.
            }
        } // End if result isn't good.

        // We're currently at a state where we have parsed a record successfully.
        // We can then reset all the intermediate state tracking, and update the indices so what's beyond here is the

        let n_fields = self.current_ends_write_index;

        // Reset all the temporary tracking stuff due to the successful parse.
        self.current_output_write_index = 0;
        self.current_ends_write_index = 0;

        // If it was an empty parse, return okay.
        if n_fields == 0 {
            return Ok((true, current_input_read_index));
        }

        /* UNCOMMENT FOR DEBUG
        eprintln!(
            "Advancing: Number of fields parsed in record = {:?}.",
            n_fields
        );

        eprintln!(
            "  -> outbuffer = {:?}",
            std::str::from_utf8(&self.output_buffer[..output_data_size]).unwrap()
        );
        eprintln!("  -> ends = {:?}", &self.ends_buffer[..n_fields]);
        */

        // At this point, we are guaranteed to have a full read of the records; we need to simply go
        // through and give each of them to the individual analyzers.

        // emit a warning if we know the headers but did not encounter the right
        // number of fields correctly
        // Note: This was inside of the if statement saying "have we figured out
        // the headers yet" below, but since retrieve_entry performs an
        // immutable borrow of self, it doesn't let me call
        // self.set_parse_warnings! Rather annoying.
        if !self.headers.is_empty() && self.num_columns != n_fields {
            self.set_parse_warnings(format!(
                "Expected {:?} columns but found {:?} in line {:?}; {}.",
                self.num_columns,
                n_fields,
                self.csv_parser.line() - 1, // We're on the next line at this point, so subtract one.
                {
                    if self.num_columns > n_fields {
                        "skipping missing fields"
                    } else {
                        "discarding excess fields"
                    }
                }
            ));
        }

        let retrieve_entry = |i: usize| -> Cow<str> {
            debug_assert_lt!(i, n_fields);
            let read_start_pos = if i == 0 { 0 } else { self.ends_buffer[i - 1] };
            let end_pos = self.ends_buffer[i];
            debug_assert_le!(read_start_pos, end_pos);
            String::from_utf8_lossy(&self.output_buffer[read_start_pos..end_pos])
            // unsafe { std::str::from_utf8_unchecked(&self.output_buffer[read_start_pos..end_pos]) }
            //     .trim() Ajit TODO
            //     .into()
        };

        // Have we figured out the headers yet?
        if !self.headers.is_empty() {
            for i in 0..usize::min(self.analyzers.len(), n_fields) {
                self.analyzers[i].add_str(&retrieve_entry(i))?;
            }
        } else {
            debug_assert_eq!(self.num_columns, 0);
            let first_line: Vec<Cow<str>> = (0..n_fields)
                .map(|s| Ok(retrieve_entry(s)))
                .collect::<Result<Vec<Cow<str>>>>()?;
            let n_columns = first_line.len();

            let has_headers = first_line
                .iter()
                .all(|s| s.is_empty() || s.parse::<f64>().is_err());

            if has_headers {
                self.headers = first_line
                    .iter()
                    .map(|s| (*s).as_ref().to_owned())
                    .collect();
            } else {
                self.headers = (0..n_columns).map(|i| format!("Column {i:?}")).collect();
            }

            // When resizing the analyzers,
            self.num_columns = first_line.len();
            self.analyzers.resize_with(
                usize::min(self.num_columns, CSV_SUMMARY_COLUMN_THRESHOLD_MAX),
                ColumnContentAnalyzer::default,
            );

            if !has_headers {
                for (i, entry) in first_line.iter().enumerate() {
                    if i >= self.analyzers.len() {
                        break;
                    }
                    self.analyzers[i].add_str(entry.trim())?;
                }
            }
        }

        Ok((true, current_input_read_index))
    }

    fn finalize_impl(&mut self) -> Result<Vec<ColumnContentSummary>> {
        // Parse any leftover material, also flushing out any remaining material in the parser state.
        let previous_leftover_tmp = take(&mut self.previous_leftover);
        let _ = self.process_next_record(&previous_leftover_tmp[..], true)?;

        let mut ret: Vec<ColumnContentSummary> = vec![];
        for a in self.analyzers.iter_mut() {
            ret.push(a.finalize()?);
        }
        // Push empty summaries on the rest.
        ret.resize_with(self.num_columns, ColumnContentSummary::default);

        Ok(ret)
    }
    fn set_parse_warnings(&mut self, warning: String) {
        match &mut self.parse_warning {
            None => self.parse_warning = Some((1, warning)),
            Some((ref mut c, ref mut w)) => {
                if *c == 1 {
                    *w = format!("  {w}\n  {warning}\n");
                } else if *c < 5 {
                    w.push_str("  ");
                    w.push_str(&warning[..]);
                    w.push('\n');
                } else if *c == 5 {
                    w.push_str("  ...\n");
                }

                *c += 1;
            }
        }
    }

    pub fn get_parse_warnings(&self) -> Option<String> {
        match &self.parse_warning {
            None => None,
            Some((1, w)) => Some(w.clone()),
            Some((c, w)) => Some(if *c <= 5 {
                format!("Encountered {:?} parsing errors:\n{}", c, &w)
            } else {
                format!("Encountered {:?} parsing errors, including:\n{}", c, &w)
            }),
        }
    }
}

// Reads the whole file from disk, and prints the CSV analysis.
// Intended to be used for small passthrough (non-pointer) files.
pub fn print_csv_summary_from_reader(file: &mut impl Read, delimiter: u8) -> Result<()> {
    let result = summarize_csv_from_reader(file, delimiter)?;
    let json = serde_json::to_string_pretty(&result)?;
    println!("{json}");
    Ok(())
}

// Reads the whole file from disk, and returns the CSV analysis.
// // Intended to be used for small passthrough (non-pointer) files.
pub fn summarize_csv_from_reader(
    file: &mut impl Read,
    delimiter: u8,
) -> Result<Option<CSVSummary>> {
    let mut analyzer = CSVAnalyzer::new(false, delimiter);

    let mut chunk: Vec<u8> = vec![0; 65536];

    loop {
        let n = file.read(&mut chunk[..])?;
        if n == 0 {
            break;
        } else {
            analyzer.process_chunk(&chunk[..n])?;
        }
    }

    let result = analyzer.finalize()?;
    Ok(result)
}

// Reads the whole file from disk, and prints the CSV analysis.
// Intended to be used for small passthrough (non-pointer) files.
pub fn print_csv_summary(file_path: &Path) -> Result<()> {
    let mut file = File::open(file_path)?;
    let ext = file_path.extension();
    let delim = if ext == Some(OsStr::new("tsv")) {
        b'\t'
    } else {
        b','
    };
    print_csv_summary_from_reader(&mut file, delim)
}

#[cfg(test)]
mod csv_tests {
    use super::{CSVAnalyzer, ColumnContentAnalyzer, ColumnContentSummary};
    use crate::errors::Result;
    use rand::{
        distributions::{Alphanumeric, Standard, Uniform},
        rngs::SmallRng,
        Rng, SeedableRng,
    };

    fn verify_csv<S: AsRef<str>, T: AsRef<str>>(
        orig_data: &[Vec<S>],
        column_names: &[T],
        chunk_segments: Vec<usize>,
    ) -> Result<()> {
        let mut csv_str: String = String::new();

        let orig_data: Vec<Vec<String>> = orig_data
            .iter()
            .map(|v| v.iter().map(|s| (*s).as_ref().to_owned()).collect())
            .collect();

        // First, go through and add in our own summaries of the data
        let mut trackers: Vec<ColumnContentAnalyzer> = Vec::new();
        trackers.resize_with(orig_data[0].len(), ColumnContentAnalyzer::default);
        for v in orig_data.iter() {
            for (i, item) in v.iter().enumerate() {
                trackers[i].add_str(item)?;
            }
        }
        let mut summaries: Vec<ColumnContentSummary> = Vec::new();
        for tr in trackers.iter_mut() {
            summaries.push(tr.finalize()?);
        }

        let mut data = orig_data;

        let column_names: Vec<String> =
            column_names.iter().map(|n| n.as_ref().to_owned()).collect();

        if !column_names.is_empty() {
            data.insert(0, column_names.clone());
        }

        for v in data.iter() {
            if !column_names.is_empty() {
                assert_eq!(column_names.len(), v.len());
            }
            assert_eq!(v.len(), data[0].len());
            for (i, item) in v.iter().enumerate() {
                csv_str.push_str(item);

                if i == v.len() - 1 {
                    csv_str.push('\n');
                } else {
                    csv_str.push(',');
                }
            }
        }

        // Do both with and without a closing newline.
        for csv_data in [
            csv_str.as_bytes(),
            csv_str.strip_suffix('\n').unwrap().as_bytes(),
        ] {
            // UNCOMMENT FOR DEBUGGING
            // eprintln!("CSV: \n{}", &csv_str);

            let mut chunks: Vec<&[u8]> = Vec::with_capacity(chunk_segments.len() + 1);

            let mut last_idx = 0;
            for s_idx in chunk_segments.iter() {
                let s_idx = *s_idx;
                if s_idx < csv_data.len() {
                    chunks.push(&csv_data[last_idx..s_idx]);
                    last_idx = s_idx;
                }
            }
            chunks.push(&csv_data[last_idx..]);

            let mut csv_anl = CSVAnalyzer::new(false, b',');

            for chunk in chunks {
                csv_anl.process_chunk(chunk)?;
            }

            let results = csv_anl.finalize_impl()?;

            // Ok, now verify that everything has been correctly detected.
            if !column_names.is_empty() {
                assert_eq!(column_names.len(), csv_anl.headers.len());
                assert_eq!(column_names.len(), csv_anl.num_columns);
            }

            for (i, (column, _pa)) in csv_anl
                .headers
                .iter()
                .zip(csv_anl.analyzers.iter())
                .enumerate()
            {
                if !column_names.is_empty() {
                    assert_eq!(*column, column_names[i]);
                } else {
                    assert_eq!(*column, format!("Column {:?}", i));
                }

                // We can't actually test these directly, as NaNs in the fields of the sketches
                // always compare to false.  So just compare the full report.
                let result = &results[i];
                assert_eq!(format!("{:?}", &summaries[i]), format!("{:?}", result));
            }
        }

        Ok(())
    }

    #[test]
    fn test_single_column_simple() -> Result<()> {
        let data = vec![vec!["V1"], vec!["V2"], vec!["V3"]];

        verify_csv(&data, &["C1"], vec![])?;

        Ok(())
    }

    #[test]
    fn test_two_column_simple() -> Result<()> {
        let data = vec![vec!["V1", "Y1"], vec!["V2", "Y2"], vec!["V3", "Y3"]];

        verify_csv(&data, &["C1", "C2"], vec![])?;

        Ok(())
    }

    #[test]
    fn test_two_column_simple_empty_colname() -> Result<()> {
        let data = vec![vec!["V1", "Y1"], vec!["V2", "Y2"], vec!["V3", "Y3"]];

        verify_csv(&data, &["", "C2"], vec![])?;

        Ok(())
    }

    fn generate_random_string_data(
        n_cols: usize,
        n_rows: usize,
        max_element_len: usize,
        insert_quotes: bool,
    ) -> Vec<Vec<String>> {
        let mut rng_impl = SmallRng::from_seed([0; 32]);
        let rng = &mut rng_impl;

        (0..n_rows)
            .map(|_| {
                (0..n_cols)
                    .map(|_| {
                        let n_chars = rng.sample(Uniform::<usize>::new(0, max_element_len));

                        let mut s: String = rng
                            .sample_iter(&Alphanumeric)
                            .take(n_chars)
                            .map(char::from)
                            .collect();

                        // Put quotes on half the strings.
                        if insert_quotes && rng.sample(Uniform::<u8>::new(0, 1)) == 1 {
                            s.insert(0, '\"');
                            s.insert(s.len(), '\"');
                        }
                        s
                    })
                    .collect()
            })
            .collect()
    }
    #[test]
    fn test_chunk_boundaries_simple() -> Result<()> {
        let data = vec![vec!["AB", "CD"], vec!["EF", "GH"]];

        for i in 0..10 {
            verify_csv(&data, &["C1", "C2"], vec![i])?;
        }

        Ok(())
    }

    #[test]
    fn test_chunk_boundaries_random_small() -> Result<()> {
        let data = generate_random_string_data(10, 10, 5, false);

        // Make sure we test a long range of chunk boundaries here.

        for i in 100..150 {
            verify_csv(
                &data,
                &((0..10).map(|i| format!("C{:?}", i)).collect::<Vec<_>>())[..],
                vec![i],
            )?;
        }

        Ok(())
    }

    #[test]
    fn test_chunk_boundaries_random_three_chunks() -> Result<()> {
        let data = generate_random_string_data(10, 10, 5, false);

        // Make sure we test a long range of chunk boundaries here.

        for i in 100..150 {
            for j in [0, 1, 10] {
                verify_csv(
                    &data,
                    &((0..10).map(|i| format!("C{:?}", i)).collect::<Vec<_>>())[..],
                    vec![i, i + j],
                )?;
            }
        }

        Ok(())
    }

    #[test]
    fn test_many_chunks() -> Result<()> {
        let data = generate_random_string_data(5, 20, 10, true);

        let mut rng_impl = SmallRng::from_seed([0; 32]);
        let rng = &mut rng_impl;

        // Make sure we test a long range of chunk boundaries here.
        for _ in 0..10 {
            // Build up random small chunks to feed to it.
            // (This could be done with fuzzing too).
            let mut boundaries = Vec::with_capacity(50);
            let mut pos = 0;
            for _ in 0..50 {
                let n = rng.sample(Uniform::<usize>::new(0, 15));
                pos += n;
                boundaries.push(pos);
            }

            verify_csv(
                &data,
                &((0..5).map(|i| format!("C{:?}", i)).collect::<Vec<_>>())[..],
                boundaries,
            )?;
        }

        Ok(())
    }

    // Force this test to only be run when --expensive_tests is added to cargo test.
    #[cfg_attr(not(feature = "expensive_tests"), ignore)]
    #[test]
    fn test_large_data_with_reallocations() -> Result<()> {
        let data = generate_random_string_data(4096, 10, 5, false);

        // Make sure we test a long range of chunk boundaries here.

        verify_csv(
            &data,
            &((0..4096).map(|i| format!("C{:?}", i)).collect::<Vec<_>>())[..],
            vec![],
        )?;

        Ok(())
    }

    // Force this test to only be run when --expensive_tests is added to cargo test.
    #[cfg_attr(not(feature = "expensive_tests"), ignore)]
    #[test]
    fn test_chunk_boundaries_random_with_quotes() -> Result<()> {
        let data = generate_random_string_data(10, 10, 5, true);

        // Make sure we test a long range of chunk boundaries here.
        for i in 100..120 {
            for j in [0, 1, 10] {
                verify_csv(
                    &data,
                    &((0..10).map(|i| format!("C{:?}", i)).collect::<Vec<_>>())[..],
                    vec![i, i + j],
                )?;
            }
        }

        Ok(())
    }

    fn generate_random_utf8_data(
        n_cols: usize,
        n_rows: usize,
        max_element_len: usize,
        insert_quotes: bool,
    ) -> Vec<Vec<String>> {
        let mut rng_impl = SmallRng::from_seed([0; 32]);
        let rng = &mut rng_impl;

        (0..n_rows)
            .map(|_| {
                (0..n_cols)
                    .map(|_| {
                        let n_chars = rng.sample(Uniform::<usize>::new(0, max_element_len));

                        let mut s: String = rng
                            .sample_iter::<char, _>(&Standard)
                            .take(n_chars)
                            .filter(|c| *c != '\"' && *c != ',' && *c != '\n' && *c != '\r')
                            .collect();

                        // Put quotes on half the strings.
                        if insert_quotes && rng.sample(Uniform::<u8>::new(0, 1)) == 1 {
                            s.insert(0, '\"');
                            s.insert(s.len(), '\"');
                        }
                        s
                    })
                    .collect()
            })
            .collect()
    }

    // Force this test to only be run when --expensive_tests is added to cargo test.
    #[cfg_attr(not(feature = "expensive_tests"), ignore)]
    #[test]
    fn test_utf8_and_chunk_boundaries() -> Result<()> {
        let data = generate_random_utf8_data(5, 10, 10, false);

        // Make sure we test a long range of chunk boundaries here.

        for i in 100..130 {
            verify_csv(
                &data,
                &((0..5).map(|i| format!("C{:?}", i)).collect::<Vec<_>>())[..],
                vec![i],
            )?;
        }

        Ok(())
    }

    // Force this test to only be run when --expensive_tests is added to cargo test.
    #[cfg_attr(not(feature = "expensive_tests"), ignore)]
    #[test]
    fn test_utf8_and_multiple_chunk_boundaries() -> Result<()> {
        let data = generate_random_utf8_data(5, 10, 10, false);

        // Make sure we test a long range of chunk boundaries here.

        for i in 100..130 {
            for j in [0, 1, 10] {
                verify_csv(
                    &data,
                    &((0..5).map(|i| format!("C{:?}", i)).collect::<Vec<_>>())[..],
                    vec![i, i + j],
                )?;
            }
        }

        Ok(())
    }

    // Force this test to only be run when --expensive_tests is added to cargo test.
    #[cfg_attr(not(feature = "expensive_tests"), ignore)]
    #[test]
    fn test_many_utf8_chunks() -> Result<()> {
        let data = generate_random_utf8_data(5, 20, 10, true);

        let mut rng_impl = SmallRng::from_seed([0; 32]);
        let rng = &mut rng_impl;

        // Make sure we test a long range of chunk boundaries here.
        for _ in 0..20 {
            // Build up random small chunks to feed to it.
            // (This could be done with fuzzing too).
            let mut boundaries = Vec::with_capacity(50);
            let mut pos = 0;
            for _ in 0..20 {
                let n = rng.sample(Uniform::<usize>::new(0, 20));
                pos += n;
                boundaries.push(pos);
            }

            verify_csv(
                &data,
                &((0..5).map(|i| format!("C{:?}", i)).collect::<Vec<_>>())[..],
                boundaries,
            )?;
        }

        Ok(())
    }
}
