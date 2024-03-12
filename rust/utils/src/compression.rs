use anyhow::anyhow;

pub const CAS_CONTENT_ENCODING_HEADER: &str = "xet-cas-content-encoding";
pub const CAS_ACCEPT_ENCODING_HEADER: &str = "xet-cas-content-encoding";

#[derive(Debug, Clone, Copy)]
pub enum CompressionScheme {
    None,
    LZ4,
}

impl Default for CompressionScheme {
    fn default() -> Self {
        CompressionScheme::None
    }
}

impl From<CompressionScheme> for &'static str {
    fn from(value: CompressionScheme) -> Self {
       match value {
           CompressionScheme::None => "none",
           CompressionScheme::LZ4 => "lz4",
       }
    }
}

impl TryFrom<&str> for CompressionScheme {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "lz4" => Ok(CompressionScheme::LZ4),
            "" | "none" => Ok(CompressionScheme::None),
            _ => Err(anyhow!("could not convert &str to CompressionScheme"))
        }
    }
}