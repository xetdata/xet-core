use mpsc::channel;
use std::io::{Read, Write};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};

pub struct PipeRead {
    recv: Receiver<Vec<u8>>,
    buf: Vec<u8>,
}

impl Read for PipeRead {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        if self.buf.is_empty() {
            if let Ok(next_buf) = self.recv.recv() {
                self.buf = next_buf;
            } else {
                // recv error, indicated channel is closed
                return Ok(0);
            }
        }
        let size = buf.write(self.buf.as_slice())?;
        if self.buf.len() == size {
            self.buf = Vec::new();
        } else {
            self.buf = self.buf.split_off(size);
        }

        Ok(size)
    }
}

pub struct PipeWrite {
    send: Sender<Vec<u8>>,
}

impl Write for PipeWrite {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let size = buf.len();
        self.send.send(buf.to_vec()).map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))?;
        Ok(size)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn pipe() -> (PipeWrite, PipeRead) {
    let (send, recv)= channel();
    let read = PipeRead {
        recv,
        buf: Vec::new(),
    };
    let write = PipeWrite {
        send,
    };
    (write, read)
}



