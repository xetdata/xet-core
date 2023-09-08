use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(ValueEnum, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SummaryType {
  Libmagic,
  Csv,
}
