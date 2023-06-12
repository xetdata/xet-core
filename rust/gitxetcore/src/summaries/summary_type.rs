use clap::arg_enum;
use serde::{Deserialize, Serialize};

arg_enum! {
  #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
  pub enum SummaryType {
      Libmagic,
      Csv,
  }
}
