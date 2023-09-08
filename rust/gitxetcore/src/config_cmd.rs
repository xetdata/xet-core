use clap::{Args, CommandFactory};
use tracing::warn;
use xet_config::{Cfg, Level};

use crate::command::GitXetCommand;
use crate::config::ConfigError::*;
use crate::config::{create_config_loader, ConfigError};
use crate::errors::GitXetRepoError;

#[derive(Args, Debug)]
pub struct ConfigArgs {
    /// use local config file
    #[clap(long, group("file-option"))]
    local: bool,

    /// use global config file
    #[clap(long, group("file-option"))]
    global: bool,

    /// unset a setting from the config
    #[clap(long, group("command"))]
    unset: bool,

    /// get a setting from the config or resolved across all commands
    #[clap(long, group("command"))]
    get: bool,

    /// list all settings from the config or resolved across all commands
    #[clap(long, group("command"))]
    list: bool,

    /// The key of a config. Should be formatted as `<section>.<name>`
    key: Option<String>,

    /// Value of the key to set.
    value: Option<String>,
}

enum Command {
    Put,
    Delete,
    Get,
    List,
}

pub fn handle_config_command(args: &ConfigArgs) -> Result<(), GitXetRepoError> {
    Ok(handle_config_command_internal(args)?)
}

fn handle_config_command_internal(args: &ConfigArgs) -> Result<(), ConfigError> {
    let command = match (args.unset, args.get, args.list) {
        (true, false, false) => Command::Delete,
        (false, true, false) => Command::Get,
        (false, false, true) => Command::List,
        (false, false, false) => {
            if args.value.is_some() || args.key.is_none() {
                Command::Put
            } else {
                Command::Get
            }
        }
        _ => return Err(InvalidCombination),
    };
    let level = match (args.local, args.global) {
        (true, false) => Level::LOCAL,
        (false, true) => Level::GLOBAL,
        (false, false) => match command {
            Command::Put | Command::Delete => Level::LOCAL,
            Command::Get | Command::List => Level::ENV,
        },
        _ => return Err(InvalidCombination),
    };

    let loader = create_config_loader().map_err(|_| ConfigLoadError)?;
    let key_ref = args.key.as_ref();

    match &command {
        Command::Put => match (key_ref, args.value.as_ref()) {
            (Some(key), Some(val)) => loader.override_value(level, key, val.as_str())?,
            (None, None) => {
                let app = GitXetCommand::command();
                let mut sub = app
                    .find_subcommand("config")
                    .ok_or_else(|| ConfigBug("can't find the config subcommand".to_string()))?
                    .clone();
                sub.print_help()?;
                return Ok(());
            }
            _ => {
                return Err(InvalidArgs(
                    "Both <KEY> and <VALUE> need to be specified when updating the config"
                        .to_string(),
                ))
            }
        },
        Command::Delete => {
            let key = key_ref.ok_or(KeyRequired)?;
            loader.remove_value(level, key)?;
        }
        Command::Get => {
            let key = key_ref.ok_or(KeyRequired)?;
            let val = match level {
                Level::LOCAL | Level::GLOBAL => loader.load_value(level, key)?,
                _ => loader.resolve_value(level, key)?,
            };
            if let Some(value) = val {
                println!("{value}");
            } else {
                warn!("key: {} not defined in config", key);
            }
        }
        Command::List => {
            if key_ref.is_some() {
                warn!("specifying a key with `--list` has no additional effect");
            }
            let cfg = match level {
                Level::LOCAL | Level::GLOBAL => loader.load_config(level)?,
                _ => loader.resolve_config(level)?,
            };
            print_config(&cfg)?;
        }
    }

    Ok(())
}

fn print_config(cfg: &Cfg) -> Result<(), ConfigError> {
    let data = cfg.to_key_value_string()?;
    println!("{data}");
    Ok(())
}
