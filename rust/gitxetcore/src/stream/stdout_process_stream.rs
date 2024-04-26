use anyhow::anyhow;
use async_trait::async_trait;
use std::io::ErrorKind;
use std::process::Stdio;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tracing::error;

use crate::errors::{GitXetRepoError, Result};
use parutils::AsyncIterator;

use super::data_iterators::AsyncDataIterator;

const CHILD_PROCESS_POLL_INTERVAL_MILLIS: u64 = 250;

/// An async data tterator that sources the data from the stdout of a process running in the background.
///
/// Example usage smudging git lfs pointer.
///
///    let mut command = tokio::process::Command::new("git");
///    command.current_dir(lfs_repo_dir).arg("lfs").arg("smudge");
///
///    // Streams out smudged data.
///    let data_out = AsyncStdoutDataIterator::from_command(command, &lfs_pointer_file[..], 16*1024*1024)
///
pub struct AsyncStdoutDataIterator {
    child_process: Option<tokio::process::Child>,
    stdout: tokio::process::ChildStdout,
    bufsize: usize,
}

impl AsyncStdoutDataIterator {
    pub async fn from_command(
        mut command: tokio::process::Command,
        stdin_data: &[u8],
        stdout_bufsize: usize,
    ) -> Result<Self> {
        let mut process = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped()) // This one to be captured.
            .spawn()?;

        // Get the stdin handle and write the data
        let Some(mut stdin) = process.stdin.take() else {
            Err(anyhow!(
                "Unable to connect stdin in child process {command:?}.",
            ))?;
            unreachable!();
        };

        stdin.write_all(stdin_data).await?; // Send all data
        stdin.flush().await?; // Ensure all data is sent

        Self::from_process(process, stdout_bufsize)
    }

    /// Constructs an Async Data Iterator reader that pipes the output from the stdout
    /// of a process into a data iterator.
    /// Reads things into an internal
    /// buffer of a given size.
    /// It is not guaranteed that every read will
    /// fill the complete buffer.
    pub fn from_process(mut child_process: tokio::process::Child, bufsize: usize) -> Result<Self> {
        // Read the command's output from stdout in a streaming manner
        let Some(stdout) = child_process.stdout.take() else {
            Err(anyhow!("Unable to connect stdout in child process."))?;
            unreachable!();
        };

        Ok(Self {
            child_process: Some(child_process),
            stdout,
            bufsize,
        })
    }
}

#[async_trait]
impl AsyncIterator<GitXetRepoError> for AsyncStdoutDataIterator {
    type Item = Vec<u8>;

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        let mut buffer = vec![0u8; self.bufsize];
        let mut broken_pipe = false;

        loop {
            if !broken_pipe {
                match self.stdout.read(&mut buffer).await {
                    Err(e) => {
                        match e.kind() {
                            ErrorKind::UnexpectedEof => {
                                // Continue on to see if the child process has completed.  If not, wait for it to finish before
                                // deciding we're done.
                            }
                            ErrorKind::Interrupted | ErrorKind::WouldBlock => {
                                // If the child process has completed, retry.  If not, then poll the child process
                                // to see if it's done and then retry.
                                if self.child_process.is_none() {
                                    tokio::time::sleep(tokio::time::Duration::from_millis(100))
                                        .await;
                                    continue;
                                }
                            }
                            ErrorKind::BrokenPipe => {
                                if self.child_process.is_none() {
                                    return Ok(None);
                                }
                                // Now, continue on to poll the exit status of the child process in case there is
                                // an error we need to propegate.  Getting this will always cause us to exit.
                                broken_pipe = true;
                            }
                            _ => {
                                // Propogate all other errors.
                                Err(e)?;
                                unreachable!();
                            }
                        };
                    }
                    Ok(readlen) => {
                        // If the readlen is 0, it could mean that the child process has paused, in which case
                        // we simply want to wait and try again.  In this situation, coninue on to poll the child
                        // process.
                        if readlen > 0 {
                            buffer.resize(readlen, 0);
                            return Ok(Some(buffer));
                        }
                    }
                }
            }

            // If we know that the child process has exited, then we're definitely complete.
            if self.child_process.is_none() {
                return Ok(None);
            }

            // If we are here, then there is no data available on the stdout of the input stream.
            // However, we only want to exit if the child process has completed.
            let child_process_completed = {
                if let Some(cp) = self.child_process.as_mut() {
                    cp.try_wait()?.is_some()
                } else {
                    false
                }
            };

            if child_process_completed {
                // First wait for the child process
                let Some(child) = self.child_process.take() else {
                    Err(anyhow!("Next called after child process completed."))?;
                    unreachable!();
                };

                let output = child.wait_with_output().await?;

                // Go back to the read until that is empty out the rest of the read pipe.
                if output.status.success() {
                    if broken_pipe {
                        return Ok(None);
                    }
                    continue;
                } else {
                    let msg = format!(
                        "Child process failed with status: {:?}: {:?}",
                        output.status, output.stderr
                    );
                    error!("{msg}");

                    Err(anyhow!("{msg}"))?;
                    unreachable!();
                }
            } else {
                // The child process is not completed yet, so wait a bit and poll again when there may be more data available.
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    CHILD_PROCESS_POLL_INTERVAL_MILLIS,
                ))
                .await;
                continue;
            }
        }
    }
}

impl AsyncDataIterator for AsyncStdoutDataIterator {}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::Result;
    use std::process::Stdio;
    use std::time::Duration;
    use tokio::process::Command;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_async_stdout_data_iterator() -> Result<()> {
        // Spawn a process that outputs data
        let child_process = Command::new("echo")
            .arg("hello")
            .stdout(Stdio::piped())
            .spawn()?;

        // Create an iterator to read the data from stdout
        let mut iterator = AsyncStdoutDataIterator::from_process(child_process, 1024)?;

        // Read from the iterator and ensure it contains the expected data
        if let Some(data) = iterator.next().await? {
            let output = String::from_utf8(data)?;
            assert_eq!(output.trim(), "hello");
        } else {
            panic!("Expected some output, but got None.");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_async_stdout_data_iterator_multiple_reads() -> Result<()> {
        // Spawn a process that outputs multiple lines
        let child_process = Command::new("bash")
            .arg("-c")
            .arg("echo 'data1'; sleep 1; echo 'data2'")
            .stdout(Stdio::piped())
            .spawn()?;

        // Create an iterator with smaller buffer size
        let mut iterator = AsyncStdoutDataIterator::from_process(child_process, 16)?;

        // Read multiple times and check the data
        if let Some(data) = iterator.next().await? {
            let output = String::from_utf8(data)?;
            assert_eq!(output.trim(), "data1");
        } else {
            panic!("Expected some output, but got None.");
        }

        sleep(Duration::from_secs(1)).await;

        if let Some(data) = iterator.next().await? {
            let output = String::from_utf8(data)?;
            assert_eq!(output.trim(), "data2");
        } else {
            panic!("Expected second output, but got None.");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_async_stdout_data_iterator_process_error() -> Result<()> {
        // Spawn a process that exits with an error
        let child_process = Command::new("bash")
            .arg("-c")
            .arg("exit 1")
            .stdout(Stdio::piped())
            .spawn()?;

        // Create an iterator
        let mut iterator = AsyncStdoutDataIterator::from_process(child_process, 1024)?;

        // Expect None because the process exited without output
        let result = iterator.next().await;
        assert!(result.is_err(), "Expected an error, but got {:?}", result);

        Ok(())
    }

    #[tokio::test]
    async fn test_async_stdout_data_iterator_from_command() -> Result<()> {
        // Spawn a process that reads from stdin and outputs to stdout
        let command = Command::new("cat"); // 'cat' simply echoes stdin to stdout
        let stdin_data = b"hello from stdin";

        // Create an iterator using the new constructor
        let mut iterator = AsyncStdoutDataIterator::from_command(command, stdin_data, 1024).await?;

        // Read from the iterator and ensure it contains the expected data
        if let Some(data) = iterator.next().await? {
            let output = String::from_utf8(data)?;
            assert_eq!(output.trim(), "hello from stdin");
        } else {
            panic!("Expected some output, but got None.");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_async_stdout_data_iterator_from_command_with_multiple_lines() -> Result<()> {
        // Spawn a process that reads from stdin and outputs multiple lines to stdout
        let mut command = Command::new("bash");
        command.arg("-c").arg("cat"); // Bash script with 'cat'
        let stdin_data = b"data1data2";

        // Create an iterator with the new constructor
        let mut iterator =
            AsyncStdoutDataIterator::from_command(command, stdin_data, "data1".len()).await?;

        // Read multiple times and check the data
        if let Some(data) = iterator.next().await? {
            let output = String::from_utf8(data)?;
            assert_eq!(output.trim(), "data1");
        } else {
            panic!("Expected 'data1', but got None.");
        }

        if let Some(data) = iterator.next().await? {
            let output = String::from_utf8(data)?;
            assert_eq!(output.trim(), "data2");
        } else {
            panic!("Expected 'data2', but got None.");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_async_stdout_data_iterator_from_command_with_empty_stdin() -> Result<()> {
        // Spawn a process that reads from stdin but sends nothing to stdout
        let command = Command::new("cat");
        let stdin_data = b""; // Empty stdin data

        // Create an iterator with the new constructor
        let mut iterator = AsyncStdoutDataIterator::from_command(command, stdin_data, 1024).await?;

        // Should return None as there's no output
        assert!(
            iterator.next().await?.is_none(),
            "Expected no output, but got some."
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_async_stdout_data_iterator_from_command_with_error() -> Result<()> {
        // Spawn a process that exits with an error
        let mut command = Command::new("bash");
        command.arg("-c").arg("exit 1"); // Bash command to exit with error

        let stdin_data = b"irrelevant"; // Data won't be used because the process exits

        // Create an iterator with the new constructor
        let mut iterator = AsyncStdoutDataIterator::from_command(command, stdin_data, 1024).await?;

        // Expect None due to early process exit
        let result = iterator.next().await;
        assert!(
            result.is_err(),
            "Expected an error due to process failure, but got {:?}",
            result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_async_stdout_data_iterator_with_pauses() -> Result<()> {
        // Spawn a process that sends some data, pauses, sends more, and then exits
        let mut command = Command::new("bash");
        command
            .arg("-c")
            .arg("echo 'first|'; sleep 1; echo 'second|'; sleep 1; echo 'final'");

        // Create an iterator with the new constructor
        let mut iterator = AsyncStdoutDataIterator::from_command(command, &[], 1).await?;

        let mut output = Vec::new();

        while let Some(data) = iterator.next().await? {
            output.extend_from_slice(&data[..]);
        }
        assert_eq!(output, b"first|\nsecond|\nfinal\n");

        Ok(())
    }
}
