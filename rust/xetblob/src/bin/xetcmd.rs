use anyhow::anyhow;
use clap::{Args, Parser, Subcommand};
use gitxetcore::config::{ConfigGitPathOption, XetConfig};
use gitxetcore::log;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use tabled::Table;
use url::Url;
use xetblob::*;

const MAX_READ_SIZE: u32 = 64 * 1024 * 1024;
struct SplitRemote {
    remote: String,
    branch: String,
    path: String,
}

fn split_remote_url(path: &str) -> anyhow::Result<SplitRemote> {
    let mut url = Url::parse(path)?;
    // example of path_segments usage from https://docs.rs/url/latest/url/index.html
    // this gets me Vec<&str>
    let path_components = url
        .path_segments()
        .map(|c| c.collect::<Vec<_>>())
        .unwrap_or_default();
    // Switch to Vec<String> as I need url to be mutable
    let path_components: Vec<String> = path_components.iter().map(|x| x.to_string()).collect();
    // it should be always [domain]/user/repo/branch/[path]
    // so there needs to be at least 3 path components
    if path_components.len() < 3 {
        return Err(anyhow!("{} is not a valid path", path));
    }
    // remote is "/user/repo
    url.set_path(&format!("/{}/{}", path_components[0], path_components[1]));
    let remote = url.to_string();
    // remaining path is everything else.
    // Note that this is safe cos we required path_components.len() >= 3 above
    let branch = path_components[2].to_string();
    let path = if path_components.len() > 3 {
        path_components[3..].join("/")
    } else {
        "".to_string()
    };
    Ok(SplitRemote {
        remote,
        branch,
        path,
    })
}

/// Lists the remote path
#[derive(Args, Debug)]
pub struct LsArgs {
    remote: String,
}

/// Lists the remote path
#[derive(Args, Debug)]
pub struct StatArgs {
    remote: String,
}

/// Lists the remote path
#[derive(Args, Debug)]
pub struct RmArgs {
    remote: String,
}

/// Copies one file from remote to local
#[derive(Args, Debug)]
pub struct CpArgs {
    src: String,
    dest: String,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Ls(LsArgs),
    Stat(StatArgs),
    Download(CpArgs),
    Upload(CpArgs),
    Cp(CpArgs),
    Mv(CpArgs),
    Rm(RmArgs),
}

#[derive(Parser, Debug)]
#[clap(about = "xetcmd command line", long_about = None)]
pub struct XetCmdCommand {
    #[clap(subcommand)]
    pub command: Command,
}

async fn run() -> anyhow::Result<()> {
    let args = XetCmdCommand::parse();
    let mut repo_manager = XetRepoManager::new(None, None)?;
    let config = XetConfig::new(None, None, ConfigGitPathOption::NoPath)?;
    let _ = log::initialize_tracing_subscriber(&config);

    match args.command {
        Command::Ls(ls) => {
            let remote_path = split_remote_url(&ls.remote)?;
            let listing = repo_manager
                .listdir(&remote_path.remote, &remote_path.branch, &remote_path.path)
                .await?;
            println!("{}", Table::new(listing));
        }
        Command::Stat(stat) => {
            let remote_path = split_remote_url(&stat.remote)?;
            let stat = repo_manager
                .stat(&remote_path.remote, &remote_path.branch, &remote_path.path)
                .await?;
            if stat.is_none() {
                println!("Not found");
            } else {
                println!("{}", Table::new(vec![stat.unwrap()]));
            }
        }
        Command::Cp(args) => {
            let src_path = split_remote_url(&args.src)?;
            let dest_path = split_remote_url(&args.dest)?;
            if src_path.remote != dest_path.remote {
                println!("src and dest must have the same remote");
            }
            let repo = repo_manager.get_repo(None, &dest_path.remote).await?;
            let mut transaction = repo
                .begin_write_transaction(&dest_path.branch, None, None)
                .await?;
            transaction
                .copy(&src_path.branch, &src_path.path, &dest_path.path)
                .await?;
            transaction.commit("hello world").await?;
        }
        Command::Mv(args) => {
            let src_path = split_remote_url(&args.src)?;
            let dest_path = split_remote_url(&args.dest)?;
            if src_path.remote != dest_path.remote {
                eprintln!("src and dest must have the same remote");
                return Ok(());
            }
            if src_path.branch != dest_path.branch {
                eprintln!("src and dest must have the same branch");
                return Ok(());
            }
            let repo = repo_manager.get_repo(None, &dest_path.remote).await?;
            let mut transaction = repo
                .begin_write_transaction(&dest_path.branch, None, None)
                .await?;
            transaction.mv(&src_path.path, &dest_path.path).await?;
            transaction.commit("hello world").await?;
        }
        Command::Rm(path) => {
            let remote_path = split_remote_url(&path.remote)?;
            let repo = repo_manager.get_repo(None, &remote_path.remote).await?;
            let mut transaction = repo
                .begin_write_transaction(&remote_path.branch, None, None)
                .await?;
            transaction.delete(&remote_path.path).await.unwrap();
            transaction.commit("hello world").await?;
        }
        Command::Download(cp) => {
            let remote_path = split_remote_url(&cp.src)?;
            let repo = repo_manager.get_repo(None, &remote_path.remote).await?;
            let remote_file = repo
                .open_for_read(&remote_path.branch, &remote_path.path, None)
                .await?;
            let len = remote_file.len();
            let mut local_file = BufWriter::new(File::create(&cp.dest)?);
            let mut readlen: u64 = 0;
            // this is terrifyingly unoptimized.
            loop {
                let (buf, eof) = remote_file.read(readlen, MAX_READ_SIZE).await?;
                readlen += buf.len() as u64;
                eprintln!("{readlen} / {len}");
                let _ = local_file.write(&buf)?;
                if eof {
                    break;
                }
            }
        }
        Command::Upload(cp) => {
            let remote_path = split_remote_url(&cp.dest)?;
            let repo = repo_manager.get_repo(None, &remote_path.remote).await?;
            let mut transaction = repo
                .begin_write_transaction(&remote_path.branch, None, None)
                .await?;
            let remote_file = transaction.open_for_write(&remote_path.path).await?;
            let mut local_file = BufReader::new(File::open(&cp.src)?);
            // this is terrifyingly unoptimized.
            loop {
                let mut buf = vec![0u8 ; MAX_READ_SIZE as usize]; 
                let len = local_file.read(&mut buf)?;
                if len == 0 {
                    break;
                }
                remote_file.write(&buf[..len]).await?;
            }
            remote_file.close().await?;
            transaction.commit("hello world").await?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    match run().await {
        Ok(_) => {}
        Err(e) => {
            eprintln!("{e:?}");
        }
    }
}
