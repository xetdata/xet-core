use std::{path::Path, str::FromStr};

use crate::error::{LazyError, Result};

#[derive(Debug, PartialEq, Clone)]
pub enum LazyStrategy {
    SMUDGE,
    POINTER,
}

impl FromStr for LazyStrategy {
    type Err = LazyError;

    fn from_str(s: &str) -> Result<Self> {
        let lower_s = s.to_ascii_lowercase();
        match lower_s.as_str() {
            "smudge" => Ok(LazyStrategy::SMUDGE),
            "pointer" => Ok(LazyStrategy::POINTER),
            "s" => Ok(LazyStrategy::SMUDGE),
            "p" => Ok(LazyStrategy::POINTER),
            _ => Err(LazyError::InvalidStrategy(format!(
                "{s}, supported strategy is smudge(s) or pointer(p)"
            ))),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct LazyRule {
    pub path: String,
    pub strategy: LazyStrategy,
}

impl FromStr for LazyRule {
    type Err = LazyError;

    fn from_str(s: &str) -> Result<Self> {
        let split = s.trim().split_once(' ').ok_or_else(|| {
            LazyError::InvalidRule(format!("failed to parse strategy from {s:?}"))
        })?;

        let strategy: LazyStrategy = split.0.parse()?;
        let path = split.1.trim().to_owned();

        Ok(LazyRule { path, strategy })
    }
}

impl LazyRule {
    pub fn match_path_prefix(&self, path: impl AsRef<Path>) -> Result<bool> {
        // TODO: break into hierarchy of folders
        let path = path
            .as_ref()
            .to_str()
            .ok_or_else(|| LazyError::InvalidPath(format!("{:?}", path.as_ref())))?;
        Ok(path.contains(&self.path))
    }
}
