use crate::level::Level;
use config::ConfigError;
use std::io;
use xet_error::Error;
use toml::ser;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum CfgError {
    #[error("Config parsing error: {0}")]
    ConfigParse(#[from] ConfigError),

    #[error("Cannot use provided value for key")]
    InvalidValue,

    #[error("Cannot modify settings for: {0}")]
    InvalidLevelModification(Level),

    #[error("Couldn't serialize config due to: {0}")]
    Serialization(#[from] ser::Error),

    #[error("Issue with disk: {0}")]
    IO(#[from] io::Error),
}
