use cas::output_bytes;
use crossterm::{cursor, QueueableCommand};
use std::io::{stderr, Write};
use std::time::Instant;

const MAX_PRINT_INTERVAL_MS: u128 = 250;

#[derive(Debug, Clone)]
pub struct DataProgressReporter {
    disable: bool,
    total_count: Option<usize>,
    current_count: usize,
    current_bytes: usize,
    message: String,
    time_at_start: Instant,
    last_print_time: Option<std::time::Instant>,
    last_write_length: usize,
    buffer: Vec<u8>,
}

impl DataProgressReporter {
    pub fn new(message: &str, total_unit_count: Option<usize>) -> Self {
        Self {
            disable: atty::isnt(atty::Stream::Stderr),
            total_count: total_unit_count,
            current_count: 0,
            current_bytes: 0,
            message: message.to_owned(),
            time_at_start: Instant::now(),
            last_print_time: None,
            last_write_length: 0,
            buffer: Vec::with_capacity(256),
        }
    }

    /// Adds progress into the register, printing the result.
    ///
    /// There are two quantities here, unit_amount and bytes.  If total_unit_count was given
    /// initially, then unit_amount should be specified to indicate how many new units were completed.
    /// Note that if no total count was specified, then this should be None.  
    ///
    /// bytes indicates the number of bytes completed along with these units.
    ///
    ///
    /// Example 1:
    ///  
    /// let mut pb = DataProgressReporter::new("Testing progress bar", Some(10));
    /// pb.register_progress(Some(1), 25 * 1024);
    /// pb.register_progress(Some(8), 50 * 1024);
    /// pb.finalize();
    ///
    ///
    /// This will update the progress display line to, in order,
    ///
    /// Testing progress bar: 10% (1/10), 25 KiB | 25 KiB/s.
    /// Testing progress bar: 90% (9/10), 75 KiB | 75 KiB/s.
    /// Testing progress bar: 100% (10/10), 75 KiB | 75 KiB/s, done.
    ///
    ///
    /// Example 2:
    ///
    ///
    /// let mut pb = DataProgressReporter::new("Testing progress bar, bytes only", None);
    /// pb.register_progress(None, 25 * 1024);
    /// pb.register_progress(None, 50 * 1024);
    /// pb.finalize();
    ///
    /// This will update the progress display line to, in order,
    ///
    /// Testing progress bar, bytes only: 25 KiB | 25 KiB/s.
    /// Testing progress bar, bytes only: 75 KiB | 75 KiB/s.
    /// Testing progress bar, bytes only: 75 KiB | 75 KiB/s, done.
    ///
    pub fn register_progress(&mut self, unit_amount: Option<usize>, bytes: usize) {
        if let Some(c) = unit_amount {
            self.current_count += c;
        }

        self.current_bytes += bytes;

        let _ = self.print(false);
    }

    pub fn update_progress(&mut self, unit_amount: Option<usize>, bytes: usize) {
        if let Some(c) = unit_amount {
            self.current_count = c;
        }

        self.current_bytes = bytes;

        let _ = self.print(false);
    }

    /// Call when done with all progress.
    pub fn finalize(&mut self) {
        let _ = self.print(true);
    }

    /// Does the actual printing
    fn print(&mut self, is_final: bool) -> std::result::Result<(), std::io::Error> {
        if self.disable {
            return Ok(());
        }
        let current_time = Instant::now();

        if !is_final {
            if let Some(t) = self.last_print_time {
                if let Some(d) = current_time.checked_duration_since(t) {
                    if d.as_millis() <= MAX_PRINT_INTERVAL_MS {
                        return Ok(());
                    }
                }
            }
        }

        const WHITESPACE: &str = "                                                    ";

        // Now, get the info.
        let byte_rate =
            self.current_bytes / (usize::max(1, self.time_at_start.elapsed().as_secs() as usize));

        let mut write_str = match self.total_count {
            Some(total_count) => {
                let percentage = usize::min(100, (100 * self.current_count) / total_count);

                format!(
                    "{}: {}% ({}/{}), {} | {}/s{}",
                    self.message,
                    if is_final { 100 } else { percentage },
                    if is_final {
                        total_count
                    } else {
                        self.current_count
                    },
                    total_count,
                    &output_bytes(self.current_bytes),
                    &output_bytes(byte_rate),
                    if is_final { ", done." } else { "." }
                )
            }
            None => {
                if self.current_count != 0 {
                    format!(
                        "{}: ({}), {} | {}/s{}",
                        self.message,
                        self.current_count,
                        &output_bytes(self.current_bytes),
                        &output_bytes(byte_rate),
                        if is_final { ", done." } else { "." }
                    )
                } else {
                    format!(
                        "{}: {} | {}/s{}",
                        self.message,
                        &output_bytes(self.current_bytes),
                        &output_bytes(byte_rate),
                        if is_final { ", done." } else { "." }
                    )
                }
            }
        };

        if write_str.len() < self.last_write_length {
            let mut len_to_write = self.last_write_length - write_str.len();

            loop {
                write_str.push_str(&WHITESPACE[..usize::min(len_to_write, WHITESPACE.len())]);
                if len_to_write > WHITESPACE.len() {
                    len_to_write -= WHITESPACE.len();
                    continue;
                } else {
                    break;
                }
            }
        }

        // Annoyingly, on windows prior to Windows 11, cursor movement is done as soon as the buffer

        self.last_write_length = write_str.len();

        let use_buffer = {
            #[cfg(windows)]
            {
                crossterm::ansi_support::supports_ansi()
            }
            #[cfg(not(windows))]
            {
                true
            }
        };

        if use_buffer {
            self.buffer.clear();
        }

        let mut stderr = stderr();

        // We haven't printed anything yet.
        if self.last_print_time.is_none() {
            // Move the cursor to the next line.  We can't seem to use cursor::MoveToNextLine
            // as it doesn't seem to add anything to the terminal buffer.  Doing a newline like
            // this seems to work fine.
            if use_buffer {
                self.buffer.write_all("\n".as_bytes())?;
            } else {
                stderr.write_all("\n".as_bytes())?;
            }
        }
        if use_buffer {
            self.buffer.queue(cursor::SavePosition)?;
            self.buffer.queue(cursor::MoveToPreviousLine(1))?;

            self.buffer.queue(cursor::MoveToColumn(0)).unwrap();

            self.buffer.write_all(write_str.as_bytes())?;

            self.buffer.queue(cursor::RestorePosition)?;

            // Finally, flush it out.
            stderr.write_all(&self.buffer)?;
        } else {
            stderr.queue(cursor::SavePosition)?;
            stderr.queue(cursor::MoveToPreviousLine(1))?;

            stderr.queue(cursor::MoveToColumn(0)).unwrap();

            stderr.write_all(write_str.as_bytes())?;

            stderr.queue(cursor::RestorePosition)?;
        }

        self.last_print_time = Some(current_time);

        Ok(())
    }
}
