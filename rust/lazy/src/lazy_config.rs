use std::{
    cmp::Ordering,
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
    pub fn load(reader: &mut impl BufRead) -> Result<Self> {
        let mut rules: Vec<LazyRule> = vec![];

        for line in reader.lines() {
            let line_content = line?;
            if line_content.trim().is_empty() {
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

    pub fn load_from_file(file: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(&file)?;
        let mut reader = BufReader::new(file);

        LazyConfig::load(&mut reader)
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
}

// * is alphabetically less than
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

    use crate::{error::Result, lazy_config::LazyRule};

    fn assert_rule_parsing(s: &str, expected: &LazyRule) -> Result<()> {
        let rule: LazyRule = s.parse()?;

        assert_eq!(&rule, expected);

        Ok(())
    }

    #[test]
    fn test_rule_parsing() -> Result<()> {
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
                path: "/home/user/a/b/c".into(),
                strategy: POINTER,
            },
        )?;

        Ok(())
    }

    #[test]
    fn test_rule_matching() -> Result<()> {
        let rules = r#"
        pointer *
        smudge !
        smudge data
        pointer data/jpegs
        smudge data/jpegs/e.jpg
        "#;

        let mut cursor = Cursor::new(rules);

        let config = LazyConfig::load(&mut cursor)?;

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
}
