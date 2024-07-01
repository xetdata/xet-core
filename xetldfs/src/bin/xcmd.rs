use anyhow;
use libc::*;
use std::fs::File;
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
struct XCommand {
    #[clap(subcommand)]
    command: Command,
}

impl XCommand {
    fn run(&self) -> anyhow::Result<()> {
        self.command.run()
    }
}

#[derive(Subcommand)]
enum Command {
    /// Equivalent to "cat".
    Cat(MultiplePathArg),

    /// Write a str from stdin to one or more files, replacing the contents if the file exists.
    Write(MultiplePathArg),

    /// Write a str from stdin to a file, appending it to the end if the file exists.
    Append(PathArg),

    /// Write a str from stdin to a file at a specific location, rewrite the contents
    /// at that location if the file exists. If the specified location is
    /// beyond the original file size, the gap between the previous file end
    /// and this location is untouched, the behavior of reading from this gap
    /// is platform dependent.
    Writeat(WriteatArgs),

    /// Get file size by calling "stat".
    Stat(PathArg),

    /// Get file size by calling "fstat".
    Fstat(PathArg),

    /// Equivalent to "cat" but uses mmap.
    CatMmap(MultiplePathArg),

    /// Same as Write, but uses mmap
    WriteMmap(MultiplePathArg),

    /// Equivalent to Writeat but uses mmap
    WriteatMmap(WriteatArgs),

    /// Equivalent to Append but uses mmap
    AppendMmap(PathArg),

    /// Testing things
    Verify,
}

#[derive(Args)]
struct PathArg {
    file: PathBuf,
}

#[derive(Args)]
struct MultiplePathArg {
    files: Vec<PathBuf>,
}

#[derive(Args)]
struct WriteatArgs {
    pos: u64,
    file: PathBuf,
}

impl Command {
    pub fn run(&self) -> anyhow::Result<()> {
        match self {
            Command::Cat(args) => cat(&args.files),
            Command::Write(args) => write(&args.files),
            Command::Append(args) => append(&args.file),
            Command::Writeat(args) => writeat(args),
            Command::Stat(args) => stat(&args.file),
            Command::Fstat(args) => fstat(&args.file),
            Command::CatMmap(args) => cat_mmap(&args.files),
            Command::WriteMmap(args) => write_mmap(&args.files),
            Command::WriteatMmap(args) => writeat_mmap(args),
            Command::AppendMmap(args) => append_mmap(&args.file),
            Command::Verify => {
                println!("VERIFICATION");
                Ok(())
            }
        }
    }
}

fn read_from_stdin() -> io::Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let mut stdin = io::stdin();
    stdin.read_to_end(&mut buffer)?;
    Ok(buffer)
}

fn cat(files: &Vec<PathBuf>) -> anyhow::Result<()> {
    for path in files {
        let contents = std::fs::read(path)?;
        let mut stdout = io::stdout();
        stdout.write_all(&contents)?;
    }

    Ok(())
}

fn write(files: &Vec<PathBuf>) -> anyhow::Result<()> {
    let data = read_from_stdin()?;

    for path in files {
        std::fs::write(path, &data)?;
    }

    Ok(())
}

fn append(path: impl AsRef<Path>) -> anyhow::Result<()> {
    let data = read_from_stdin()?;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;

    file.write(&data)?;

    Ok(())
}

fn writeat(args: &WriteatArgs) -> anyhow::Result<()> {
    let data = read_from_stdin()?;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&args.file)?;

    file.seek(io::SeekFrom::Start(args.pos))?;
    file.write(&data)?;

    Ok(())
}

fn stat(path: impl AsRef<Path>) -> anyhow::Result<()> {
    let meta = std::fs::metadata(path)?;

    println!("{}", meta.len());

    Ok(())
}

fn fstat(path: impl AsRef<Path>) -> anyhow::Result<()> {
    let file = std::fs::OpenOptions::new().read(true).open(path)?;
    let meta = file.metadata()?;

    println!("{}", meta.len());

    Ok(())
}

use libc::{c_void, mmap, munmap, size_t, MAP_PRIVATE, PROT_READ};

use std::os::unix::io::AsRawFd;

fn read_file_with_mmap(file_path: impl AsRef<Path>) -> std::io::Result<Vec<u8>> {
    // Open the file
    let file = File::open(file_path)?;
    let fd = file.as_raw_fd();

    // Get the file length
    let metadata = file.metadata()?;
    let file_length = metadata.len() as size_t;

    // Memory map the file
    let mmap_addr = unsafe {
        mmap(
            std::ptr::null_mut(),
            file_length,
            PROT_READ,
            MAP_PRIVATE,
            fd,
            0,
        ) as *mut u8
    };

    if mmap_addr == libc::MAP_FAILED as *mut u8 {
        return Err(io::Error::last_os_error());
    }

    // Read the file content from the mapped memory
    let mut buffer = Vec::with_capacity(file_length);
    unsafe {
        buffer.extend_from_slice(std::slice::from_raw_parts(mmap_addr, file_length));
    }

    // Unmap the memory
    unsafe {
        munmap(mmap_addr as *mut c_void, file_length);
    }

    Ok(buffer)
}

fn cat_mmap(files: &Vec<PathBuf>) -> anyhow::Result<()> {
    let mut out_data = Vec::<Vec<u8>>::new();

    for path in files {
        let data = read_file_with_mmap(path)?;
        io::stdout().write_all(&data)?;
        out_data.push(data);
    }

    for (path, data) in files.iter().zip(out_data.iter()) {
        eprintln!("{path:?}: {:?}", std::str::from_utf8(data).unwrap());
    }

    Ok(())
}

enum WriteMode {
    WriteAtEnd,
    WriteFromPos(usize),

    OverwriteFile,
}

fn write_to_file_with_mmap(
    file_path: impl AsRef<Path>,
    write_at: WriteMode,
    data: &[u8],
) -> std::io::Result<()> {
    // Open the file
    let file = File::options().read(true).write(true).open(file_path)?;
    let fd = file.as_raw_fd();

    // Get the file length
    let metadata = file.metadata()?;
    let mut file_length = metadata.len() as size_t;

    let (offset, new_file_length) = match write_at {
        WriteMode::WriteAtEnd => (file_length, file_length + data.len()),
        WriteMode::WriteFromPos(p) => (p, file_length.max(p + data.len())),
        WriteMode::OverwriteFile => (0, data.len()),
    };

    if new_file_length != file_length {
        file.set_len(new_file_length as u64)?;
        file_length = new_file_length;
    }

    // Memory map the file
    let mmap_addr = unsafe {
        mmap(
            std::ptr::null_mut(),
            file_length,
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            fd,
            0,
        ) as *mut u8
    };

    if mmap_addr == libc::MAP_FAILED as *mut u8 {
        return Err(io::Error::last_os_error());
    }

    // Write the data to the mapped memory
    unsafe {
        let mmap_slice = std::slice::from_raw_parts_mut(mmap_addr, file_length);
        mmap_slice[offset..offset + data.len()].copy_from_slice(data);
    }

    // Unmap the memory
    unsafe {
        munmap(mmap_addr as *mut c_void, file_length);
    }

    Ok(())
}

fn write_mmap(files: &Vec<PathBuf>) -> anyhow::Result<()> {
    let data = read_from_stdin()?;

    for path in files {
        write_to_file_with_mmap(&path, WriteMode::OverwriteFile, &data)?;
        std::fs::write(path, &data)?;
    }

    Ok(())
}

fn writeat_mmap(args: &WriteatArgs) -> anyhow::Result<()> {
    let data = read_from_stdin()?;
    write_to_file_with_mmap(
        &args.file,
        WriteMode::WriteFromPos(args.pos as usize),
        &data,
    )?;

    Ok(())
}

fn append_mmap(path: impl AsRef<Path>) -> anyhow::Result<()> {
    let data = read_from_stdin()?;
    write_to_file_with_mmap(path, WriteMode::WriteAtEnd, &data)?;

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let cli = XCommand::parse();
    cli.run()
}
