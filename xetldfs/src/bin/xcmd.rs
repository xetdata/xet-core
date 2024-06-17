use std::{io::Write, path::PathBuf};

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
    /// Write a str to a file, replacing the contents if the file exists.
    Write(WriteArgs),
    /// Write a str to a file, appending it to the end if the file exists.
    Append(AppendArgs),
    /// Equivalent to "echo".
    Echo(EchoArgs),
}

#[derive(Args)]
struct CatArgs {
    file: PathBuf,
}

#[derive(Args)]
struct WriteArgs {
    str: String,
    file: PathBuf,
}

#[derive(Args)]
struct AppendArgs {
    str: String,
    file: PathBuf,
}

#[derive(Args)]
struct EchoArgs {
    /// Do not print the trailing newline character.
    n: bool,
    strs: Vec<String>,
}

impl Command {
    pub fn run(&self) -> anyhow::Result<()> {
        match self {
            Command::Cat(args) => cat(args),
            Command::Write(args) => write(args),
            Command::Append(args) => append(args),
            Command::Echo(args) => echo(args),
        }
    }
}

fn cat(args: &CatArgs) -> anyhow::Result<()> {
    let contents = std::fs::read_to_string(&args.file)?;

    print!("{}", contents);

    Ok(())
}

fn write(args: &WriteArgs) -> anyhow::Result<()> {
    std::fs::write(&args.file, &args.str)?;

    Ok(())
}

fn append(args: &AppendArgs) -> anyhow::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.file)?;

    file.write(args.str.as_bytes())?;

    Ok(())
}

fn echo(args: &EchoArgs) -> anyhow::Result<()> {
    let s = args.strs.join(" ");
    if args.n {
        print!("{s}",);
    } else {
        println!("{s}");
    }

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let cli = XCommand::parse();
    cli.run()
}
