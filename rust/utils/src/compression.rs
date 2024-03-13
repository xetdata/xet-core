use std::str::FromStr;
use anyhow::anyhow;

pub const CAS_CONTENT_ENCODING_HEADER: &str = "xet-cas-content-encoding";
pub const CAS_ACCEPT_ENCODING_HEADER: &str = "xet-cas-content-encoding";

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub enum CompressionScheme {
    #[default]
    None,
    LZ4,
}

impl From<&CompressionScheme> for &'static str {
    fn from(value: &CompressionScheme) -> Self {
       match value {
           CompressionScheme::None => "none",
           CompressionScheme::LZ4 => "lz4",
       }
    }
}

impl From<CompressionScheme> for &'static str {
    fn from(value: CompressionScheme) -> Self {
        From::from(&value)
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

pub fn multiple_accepted_encoding_header_value(list: Vec<CompressionScheme>) -> String {
    let as_strs: Vec<&str> = list.iter().map(Into::into).collect();
    as_strs.join(";").to_string()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use crate::compression::{CompressionScheme, multiple_accepted_encoding_header_value};

    #[test]
    fn test_from_str() {
        assert_eq!(CompressionScheme::from_str("lz4").unwrap(), CompressionScheme::LZ4);
        assert_eq!(CompressionScheme::from_str("none").unwrap(), CompressionScheme::None);
        assert_eq!(CompressionScheme::from_str("").unwrap(), CompressionScheme::None);
        assert!(CompressionScheme::from_str("not-scheme").is_err());
    }

    #[test]
    fn test_to_str() {
        assert_eq!(Into::<&str>::into(CompressionScheme::LZ4), "lz4");
        assert_eq!(Into::<&str>::into(CompressionScheme::None), "none");
    }

    #[test]
    fn test_multiple_accepted_encoding_header_value() {
        let multi = vec![CompressionScheme::LZ4, CompressionScheme::None];
        assert_eq!(multiple_accepted_encoding_header_value(multi), "lz4;none".to_string());

        let singular = vec![CompressionScheme::LZ4];
        assert_eq!(multiple_accepted_encoding_header_value(singular), "lz4".to_string());
    }
}