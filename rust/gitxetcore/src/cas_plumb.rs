use std::{fs, io, io::Write};

use clap::{Args, Subcommand};
use colored::Colorize;

use cas_client::{CasClientError, MerkleHash};

use crate::config::XetConfig;
use crate::data_processing::PointerFileTranslator;
use crate::errors;
use crate::standalone_pointer::*;
use cas::output_bytes;

/// Probes the staging and remote CAS server for the existence of the XORB
#[derive(Args, Debug)]
pub struct ProbeArgs {
    hash: String,
}

/// Pushes the XORBs in the staging CAS to the remote CAS server with the option to
/// retain them.
#[derive(Args, Debug)]
pub struct StagePushArgs {
    #[clap(long, short, parse(try_from_str), default_value = "false")]
    retain: bool,
}

/// Gets the data contained in the XORB and prints it to stdout.
#[derive(Args, Debug)]
pub struct GetArgs {
    hash: String,
}

/// Plumbing commands for the Content Address Storages (CAS)
#[derive(Subcommand, Debug)]
enum CasCommand {
    /// Prints out diagnostic information for the CAS.
    Status,
    Probe(ProbeArgs),
    /// Lists all of the items in the staging CAS.
    StageList,
    StagePush(StagePushArgs),
    Get(GetArgs),
    /// Reads a file from stdin and writes out a standalone pointer to stdout.
    /// Staging Xorbs may be created.
    #[clap(hide(true))]
    CleanToStandalone,
    /// Reads a standalone pointer from stdin and writes the materialized file to stdout
    #[clap(hide(true))]
    SmudgeFromStandalone,
}

impl CasSubCommandShim {
    pub fn subcommand_name(&self) -> String {
        match self.subcommand {
            CasCommand::Status => "status".to_string(),
            CasCommand::Probe(_) => "probe".to_string(),
            CasCommand::StageList => "stage_list".to_string(),
            CasCommand::StagePush(_) => "stage_push".to_string(),
            CasCommand::Get(_) => "get".to_string(),
            CasCommand::CleanToStandalone => "clean_to_standalone".to_string(),
            CasCommand::SmudgeFromStandalone => "smudge_from_standalone".to_string(),
        }
    }
}

#[derive(Args, Debug)]
pub struct CasSubCommandShim {
    #[clap(subcommand)]
    subcommand: CasCommand,
}

/// Get a status of our CAS settings. This routine checks the remote
/// endpoint, the prefix, the cache size, and the staging size.
async fn cas_status(config: &XetConfig, repo: &PointerFileTranslator) {
    println!(
        "{} {}",
        "CAS remote:".to_string().bright_blue().bold(),
        config.cas.endpoint
    );
    println!(
        "{} {}",
        "CAS prefix:".to_string().bright_blue().bold(),
        repo.get_prefix()
    );
    if config.cache.enabled {
        println!(
            "{} {} / {}",
            "Cache Size:".to_string().bright_blue().bold(),
            output_bytes(dir_size(fs::read_dir(&config.cache.path).unwrap()).unwrap()).bold(),
            output_bytes(config.cache.size as usize)
        );
    } else {
        println!("{}", "Local cache disabled".red());
    }
    println!(
        "{} {}",
        "Staging Size:".to_string().bright_blue().bold(),
        output_bytes(repo.get_cas().get_staging_size().unwrap()),
    );
}

/// Search for the hash provided as an argument. This routine will check
/// staging and the remote for the object.
async fn cas_probe(repo: &PointerFileTranslator, probe_args: &ProbeArgs) {
    let hash = match MerkleHash::from_hex(&probe_args.hash[..]) {
        Ok(v) => v,
        Err(e) => {
            println!(
                "{} {}",
                "Invalid merkle hash format.".to_string().red(),
                format!("{e}").bright_red()
            );
            return;
        }
    };

    println!(
        "{} {} {}",
        "Probing for hash".to_string().white(),
        hash.to_string().green(),
        "...".to_string().white()
    );

    // check staging
    match repo
        .get_cas()
        .get_length_staged(&repo.get_prefix(), &hash)
        .await
    {
        Ok(size) => println!(
            "{}\n\t{}\n\t{} {}",
            "Staging:".to_string().bright_blue(),
            "√ Available".to_string().green(),
            "File Size:".to_string().cyan(),
            output_bytes(size).bold(),
        ),
        Err(err) => match err {
            CasClientError::XORBNotFound(_) | CasClientError::Grpc(_) => {
                println!(
                    "{}\n\t{}\n",
                    "Staging: ".to_string().bright_blue(),
                    "◯ Not available".to_string().yellow()
                )
            }
            _ => eprintln!(
                "{}",
                format!("Unexpected error: {err} while accessing staging.").red()
            ),
        },
    }

    // check remote
    match repo
        .get_cas()
        .get_length_remote(&repo.get_prefix(), &hash)
        .await
    {
        Ok(size) => println!(
            "{}\n\t{}\n\t{} {}",
            "Remote:".to_string().bright_blue(),
            "√ Available".to_string().green(),
            "File Size:".to_string().cyan(),
            output_bytes(size).bold(),
        ),
        Err(err) => match err {
            CasClientError::XORBNotFound(_) | CasClientError::Grpc(_) => {
                println!(
                    "{}\n\t{}",
                    "Remote: ".to_string().bright_blue(),
                    "◯ Not available".to_string().yellow()
                )
            }
            _ => eprintln!(
                "{}",
                format!("Unexpected error: {err} while accessing remote.").red()
            ),
        },
    }
}

/// Prints out a list of items contained in staging. It will only list valid XORB
/// objects.
async fn cas_stage_list(repo: &PointerFileTranslator) {
    // get the staging object
    let items: Vec<String> = repo.get_cas().list_all_staged().await.unwrap();

    if items.is_empty() {
        println!("{}", "Staging is empty.".to_string().yellow());
        return;
    }

    println!(
        "{}",
        "Items contained in staging:".to_string().blue().bold()
    );

    // iterate through the objects and print them
    for hash in items {
        println!("\t{hash}")
    }
}

/// Pushes all in staging to the remote, with the option of retaining them on push.
async fn cas_stage_push(repo: &PointerFileTranslator, stage_push_args: &StagePushArgs) {
    println!(
        "{}",
        "Pushing staging to remote...".to_string().blue().bold()
    );
    if stage_push_args.retain {
        println!("{}", "Retaining objects.".to_string().white());
    }

    repo.upload_cas_staged(stage_push_args.retain)
        .await
        .unwrap();
    println!("{}", "√ Successfully uploaded objects.".to_string().green())
}

/// Gets a CAS from the server and writes it to stdout
async fn cas_get(repo: &PointerFileTranslator, get_args: &GetArgs) {
    let hash = match MerkleHash::from_hex(&get_args.hash[..]) {
        Ok(v) => v,
        Err(e) => {
            println!(
                "{}\n\n\t{}",
                "Could not parse hash due to error:".to_string().red(),
                format!("{e}").bright_red()
            );
            return;
        }
    };

    let data = match repo.get_cas().get(&repo.get_prefix(), &hash).await {
        Ok(v) => v,
        Err(e) => {
            println!(
                "{}\n\n\t{}",
                "Could not retrieve XORB due to error:".to_string().red(),
                format!("{e}").bright_red()
            );
            return;
        }
    };

    io::stdout().write_all(&data).unwrap();
}

/// Parse and process the CAS plumbing commands for the command line. These commands
/// enable insight into things related to staging, the cache, and CAS cloud storage.
pub async fn handle_cas_plumb_command(
    config: &XetConfig,
    command: &CasSubCommandShim,
) -> errors::Result<()> {
    let repo = PointerFileTranslator::from_config(config).await?;
    match &command.subcommand {
        CasCommand::Get(args) => cas_get(&repo, args).await,
        CasCommand::Status => cas_status(config, &repo).await,
        CasCommand::StageList => cas_stage_list(&repo).await,
        CasCommand::StagePush(args) => cas_stage_push(&repo, args).await,
        CasCommand::Probe(args) => cas_probe(&repo, args).await,
        CasCommand::CleanToStandalone => {
            file_to_standalone_pointer(config, std::io::stdin(), std::io::stdout()).await?
        }
        CasCommand::SmudgeFromStandalone => {
            standalone_pointer_to_file(config, std::io::stdin(), std::io::stdout()).await?
        }
    }
    Ok(())
}

/// Recursively traverses a directory and returns a sum of all of the files contained
/// within. Returns a Ok(usize) with the size in bytes and Err(_) otherwise.
///
/// # Arguments
/// * `dir` - the directory to get the file size of as an fs::ReadDir struct.
fn dir_size(mut dir: fs::ReadDir) -> io::Result<usize> {
    dir.try_fold(0, |acc, file| {
        let file = file?;
        let size = match file.metadata()? {
            data if data.is_dir() => dir_size(fs::read_dir(file.path())?)?,
            data => data.len() as usize,
        };
        Ok(acc + size)
    })
}
