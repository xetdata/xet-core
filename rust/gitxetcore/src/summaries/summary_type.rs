use std::str::FromStr;
use anyhow::anyhow;
use clap::ArgEnum;
use serde::{Deserialize, Serialize};

#[derive(ArgEnum, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SummaryType {
  Libmagic,
  Csv,
}

impl FromStr for SummaryType {
  type Err = anyhow::Error;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s.to_lowercase().as_str() {
      "libmagic" => Ok(SummaryType::Libmagic),
      "csv" => Ok(SummaryType::Csv),
      _ => Err(anyhow!("Cannot parse {s} as SummaryType")),
    }
  }
}
