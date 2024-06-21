use std::{
    io::{self, Seek, Write},
    path::PathBuf,
};

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
    Cat(CatArgs),
    /// Write a str from stdin to a file, replacing the contents if the file exists.
    Write(WriteArgs),
    /// Write a str from stdin to a file, appending it to the end if the file exists.
    Append(AppendArgs),
    /// Write a str from stdin to a file at a specific location, rewrite the contents
    /// at that location if the file exists. If the specified location is
    /// beyond the original file size, the gap between the previous file end
    /// and this location is untouched, the behavior of reading from this gap
    /// is platform dependent.
    Writeat(WriteatArgs),
    /// Get file size by calling "stat".
    Stat(StatArgs),
    /// Get file size by calling "fstat".
    Fstat(StatArgs),
}

#[derive(Args)]
struct CatArgs {
    file: PathBuf,
}

#[derive(Args)]
struct WriteArgs {
    file: PathBuf,
}

#[derive(Args)]
struct AppendArgs {
    file: PathBuf,
}

#[derive(Args)]
struct WriteatArgs {
    pos: u64,
    file: PathBuf,
}

#[derive(Args)]
struct StatArgs {
    file: PathBuf,
}

impl Command {
    pub fn run(&self) -> anyhow::Result<()> {
        match self {
            Command::Cat(args) => cat(args),
            Command::Write(args) => write(args),
            Command::Append(args) => append(args),
            Command::Writeat(args) => writeat(args),
            Command::Stat(args) => stat(args),
            Command::Fstat(args) => fstat(args),
        }
    }
}

fn cat(args: &CatArgs) -> anyhow::Result<()> {
    let contents = std::fs::read_to_string(&args.file)?;

    print!("{}", contents);

    Ok(())
}

fn write(args: &WriteArgs) -> anyhow::Result<()> {
    let content = std::io::read_to_string(io::stdin())?;

    std::fs::write(&args.file, &content)?;

    Ok(())
}

fn append(args: &AppendArgs) -> anyhow::Result<()> {
    let content = std::io::read_to_string(io::stdin())?;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.file)?;

    file.write(content.as_bytes())?;

    Ok(())
}

fn writeat(args: &WriteatArgs) -> anyhow::Result<()> {
    let content = std::io::read_to_string(io::stdin())?;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&args.file)?;

    file.seek(io::SeekFrom::Start(args.pos))?;
    file.write(content.as_bytes())?;

    Ok(())
}

fn stat(args: &StatArgs) -> anyhow::Result<()> {
    let meta = std::fs::metadata(&args.file)?;

    println!("{}", meta.len());

    Ok(())
}

fn fstat(args: &StatArgs) -> anyhow::Result<()> {
    let file = std::fs::OpenOptions::new().read(true).open(&args.file)?;
    let meta = file.metadata()?;

    println!("{}", meta.len());

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let cli = XCommand::parse();
    cli.run()
}
