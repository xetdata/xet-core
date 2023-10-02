use std::{
    cmp::Ordering,
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader, Write},
    path::Path,
    vec,
};

use crate::error::{LazyError, Result};
pub use crate::lazy_rule::*;

#[derive(Debug)]
pub struct LazyConfig {
    rules: Vec<LazyRule>,
}

impl LazyConfig {
    pub async fn load(reader: &mut impl BufRead) -> Result<Self> {
        let mut rules: Vec<LazyRule> = vec![];

        for line in reader.lines() {
            let line_content = line?;
            // empty lines
            if line_content.trim().is_empty() {
                continue;
            }
            // comments
            if line_content.trim().starts_with('#') {
                continue;
            }
            let rule: LazyRule = line_content.parse()?;
            rules.push(rule);
        }

        // sort the rules by alphabetical order, and wildcard always at the top
        rules.sort_by(|a, b| {
            if a.path == "*" {
                Ordering::Less
            } else if b.path == "*" {
                Ordering::Greater
            } else {
                a.path.cmp(&b.path)
            }
        });

        Ok(Self { rules })
    }

    pub async fn load_from_file(file: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(&file)?;
        let mut reader = BufReader::new(file);

        LazyConfig::load(&mut reader).await
    }

    /// Given a path, checks which rule is applied.
    pub fn match_rule(&self, path: impl AsRef<Path>) -> Result<LazyRule> {
        for r in self.rules.iter().rev() {
            if r.match_path_prefix(&path)? {
                return Ok(r.clone());
            }
        }

        if self.rules[0].path != "*" {
            Err(LazyError::NoMatchingRule(format!("{:?}", path.as_ref())))
        } else {
            Ok(self.rules[0].clone())
        }
    }

    /// Check if a config has conflicting rules
    /// i.e. rules with same target path but different strategy.
    pub fn check(&self) -> Result<()> {
        let mut ht = HashMap::new();

        for r in self.rules.iter() {
            if let Some(entry) = ht.get(&r.path) {
                if entry == &r.strategy {
                    println!("Duplicate rule {r:?}");
                } else {
                    return Err(LazyError::InvalidRule(format!("Conflicting rule {r:?}")));
                }
            } else {
                ht.insert(r.path.clone(), r.strategy.clone());
            }
        }

        Ok(())
    }
}

const DEFAULT_LAZY_RULE: &str = "pointer *";

pub fn write_default_lazy_config(config_file: &Path) -> Result<()> {
    let mut file = File::create(config_file)?;
    file.write_all(DEFAULT_LAZY_RULE.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, vec};

    use super::{LazyConfig, LazyStrategy::*};

    use crate::error::Result;

    #[tokio::test]
    async fn test_rule_match() -> Result<()> {
        let rules = r#"
        pointer *
        smudge !
        # this is a comment
        smudge data
        pointer data/jpegs
        smudge data/jpegs/e.jpg
        "#;

        let mut cursor = Cursor::new(rules);

        let config = LazyConfig::load(&mut cursor).await?;

        println!("{config:?}");

        // basic path
        let queries_and_expected = vec![
            ("a.csv", POINTER),
            ("b.csv", POINTER),
            ("data/c.mp3", SMUDGE),
            ("data/d.flac", SMUDGE),
            ("data/jpegs/e.jpg", SMUDGE),
            ("data/jpegs/f.jpg", POINTER),
        ];

        for (q, e) in queries_and_expected {
            let matched_rule = config.match_rule(q)?;
            println!("{q:?}, {e:?}");
            println!("{matched_rule:?}");
            assert_eq!(matched_rule.strategy, e);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_rule_check() -> Result<()> {
        let rules = r#"
        pointer *
        smudge data
        pointer data
        "#;

        let mut cursor = Cursor::new(rules);

        let config = LazyConfig::load(&mut cursor).await?;

        // Conflicting rules exist
        assert!(config.check().is_err());

        Ok(())
    }
}
