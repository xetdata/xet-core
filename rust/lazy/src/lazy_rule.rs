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
        let path = split
            .1
            .trim()
            .trim_matches(std::path::MAIN_SEPARATOR)
            .to_owned();

        Ok(LazyRule { path, strategy })
    }
}

impl LazyRule {
    pub fn match_path_prefix(&self, path: impl AsRef<Path>) -> Result<bool> {
        let path = path
            .as_ref()
            .to_str()
            .ok_or_else(|| LazyError::InvalidPath(format!("{:?}", path.as_ref())))?;

        // rule target equals path OR
        // rule target is prefix of path AND
        // the next character in path is a directory separator
        let is_match = path == self.path
            || (path.starts_with(&self.path)
                && path.chars().nth(self.path.len()) == Some(std::path::MAIN_SEPARATOR));

        Ok(is_match)
    }
}

mod tests {
    use super::LazyRule;
    #[allow(unused_imports)]
    use super::LazyStrategy::*;
    use crate::error::Result;

    #[allow(dead_code)]
    fn assert_rule_parsing(s: &str, expected: &LazyRule) -> Result<()> {
        let rule: LazyRule = s.parse()?;

        assert_eq!(&rule, expected);

        Ok(())
    }

    #[allow(dead_code)]
    fn assert_rule_matching(rule: &str, s: &str, is_match: bool) -> Result<()> {
        let rule: LazyRule = rule.parse()?;

        assert_eq!(rule.match_path_prefix(s)?, is_match);

        Ok(())
    }

    #[test]
    fn test_rule_parse() -> Result<()> {
        assert_rule_parsing(
            "smudge data",
            &LazyRule {
                path: "data".into(),
                strategy: SMUDGE,
            },
        )?;

        assert_rule_parsing(
            "pointeR  images/1.jpg",
            &LazyRule {
                path: "images/1.jpg".into(),
                strategy: POINTER,
            },
        )?;

        assert_rule_parsing(
            " s data ",
            &LazyRule {
                path: "data".into(),
                strategy: SMUDGE,
            },
        )?;

        assert_rule_parsing(
            " P  /home/user/a/b/c",
            &LazyRule {
                path: "home/user/a/b/c".into(),
                strategy: POINTER,
            },
        )?;

        Ok(())
    }

    #[test]
    fn test_rule_match() -> Result<()> {
        assert_rule_matching("smudge data", "data/f0", true)?;

        assert_rule_matching("smudge data/", "data/f0", true)?;

        assert_rule_matching("smudge dat", "data/f0", false)?;

        Ok(())
    }
}
