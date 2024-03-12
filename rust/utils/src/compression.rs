use std::str::FromStr;
use anyhow::anyhow;

pub const CAS_CONTENT_ENCODING_HEADER: &str = "xet-cas-content-encoding";
pub const CAS_ACCEPT_ENCODING_HEADER: &str = "xet-cas-content-encoding";

#[derive(Debug, Default, Clone, Copy)]
pub enum CompressionScheme {
    #[default]
    None,
    LZ4,
}

impl From<CompressionScheme> for &'static str {
    fn from(value: CompressionScheme) -> Self {
       match value {
           CompressionScheme::None => "none",
           CompressionScheme::LZ4 => "lz4",
       }
    }
}

impl FromStr for CompressionScheme {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "lz4" => Ok(CompressionScheme::LZ4),
            "" | "none" => Ok(CompressionScheme::None),
            _ => Err(anyhow!("could not convert &str to CompressionScheme"))
        }
    }
}