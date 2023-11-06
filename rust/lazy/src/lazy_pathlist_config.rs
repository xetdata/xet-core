use std::{
    collections::HashSet,
    fs::File,
    io::{prelude::*, BufRead, BufReader},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
};

use tracing::{error, info};

use crate::{error::Result, lazy_rule::LazyStrategy};

#[derive(Debug)]
pub struct LazyPathListConfig {
    default_strategy: LazyStrategy,
    inlist_strategy: LazyStrategy,
    pathlist: HashSet<String>,
    changed: bool,
}

impl LazyPathListConfig {
    pub async fn load(
        reader: &mut impl BufRead,
        warns_duplicate: bool,
        default_strategy: LazyStrategy,
        inlist_strategy: LazyStrategy,
    ) -> Result<Self> {
        let mut pathlist: HashSet<String> = Default::default();

        for line in reader.lines() {
            let line_content = line?;
            let line_content = line_content.trim();
            // empty lines
            if line_content.is_empty() {
                continue;
            }
            // comments
            if line_content.starts_with('#') {
                continue;
            }

            if !pathlist.insert(line_content.to_owned()) {
                // already contained this line
                if warns_duplicate {
                    println!("Found duplicate path {line_content:?} in the list.");
                }
            }
        }

        Ok(Self {
            pathlist,
            default_strategy,
            inlist_strategy,
            changed: false,
        })
    }

    /// Given a path, checks if it is in the list.
    pub fn match_rule(&self, path: impl AsRef<Path>) -> LazyStrategy {
        if self
            .pathlist
            .get(path.as_ref().to_str().unwrap_or_default())
            .is_some()
        {
            self.inlist_strategy.clone()
        } else {
            self.default_strategy.clone()
        }
    }

    /// Adds a list of paths to the list.
    pub fn add_rules(&mut self, path: &[&str]) {
        for p in path {
            self.pathlist.insert((*p).to_owned());
        }
        self.changed = true;
    }

    /// Drops a list of paths from the list.
    pub fn drop_rules(&mut self, path: &[&str]) {
        for p in path {
            // ignores if the path to drop is not in the list
            self.pathlist.remove(*p);
        }
        self.changed = true;
    }
}

#[derive(Debug)]
pub struct LazyPathListConfigFile {
    config: LazyPathListConfig,
    config_file_path: PathBuf,
}

impl LazyPathListConfigFile {
    pub async fn load_from_file(
        path: impl AsRef<Path>,
        warns_duplicate: bool,
        default_strategy: LazyStrategy,
        inlist_strategy: LazyStrategy,
    ) -> Result<Self> {
        let file = File::open(&path)?;
        let mut reader = BufReader::new(file);

        let config = LazyPathListConfig::load(
            &mut reader,
            warns_duplicate,
            default_strategy,
            inlist_strategy,
        )
        .await?;

        Ok(Self {
            config,
            config_file_path: path.as_ref().to_path_buf(),
        })
    }

    /// Loads a config file as a list of file paths for smudge / materialize.
    /// Any file with path not in the list is intended to be kept as pointer file.
    pub async fn load_smudge_list_from_file(
        file: impl AsRef<Path>,
        warns_duplicate: bool,
    ) -> Result<Self> {
        Self::load_from_file(
            file,
            warns_duplicate,
            LazyStrategy::POINTER,
            LazyStrategy::SMUDGE,
        )
        .await
    }
}

impl Deref for LazyPathListConfigFile {
    type Target = LazyPathListConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl DerefMut for LazyPathListConfigFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.config
    }
}

impl Drop for LazyPathListConfigFile {
    fn drop(&mut self) {
        if !self.config.changed {
            return;
        }

        info!(
            "Updating lazy config changes to {:?}",
            self.config_file_path
        );

        let mut buf = vec![];
        #[allow(clippy::blocks_in_if_conditions)]
        for p in self.config.pathlist.iter() {
            if writeln!(&mut buf, "{p}")
                .map_err(|e| {
                    error!("Error writing lazy config path {p:?}: {e:?}");
                    e
                })
                .is_err()
            {
                return;
            };
        }

        let _ = std::fs::write(&self.config_file_path, buf).map_err(|e| {
            error!(
                "Error writing lazy config to file {:?}: {e:?}",
                &self.config_file_path
            );
            e
        });
    }
}

/// Check if the config file exists and if parsing succeeds.
/// Otherwise create the default empty config.
pub async fn check_or_create_lazy_config(config_file: impl AsRef<Path>) -> Result<()> {
    let config_file = config_file.as_ref();
    if config_file.is_file() {
        LazyPathListConfigFile::load_smudge_list_from_file(config_file, false).await?;
        return Ok(());
    }

    info!("Creating lazy config file at {config_file:?}");
    File::create(config_file)?;
    Ok(())
}

/// Print everything in the config file to stdout without parsing,
/// i.e. keep all spaces and comments and formatting.
pub fn print_lazy_config(config_file: impl AsRef<Path>) -> Result<()> {
    let config_file = config_file.as_ref();
    println!("{}", std::fs::read_to_string(config_file)?);

    Ok(())
}

#[cfg(test)]
mod test {
    use std::{io::prelude::*, io::Cursor};

    use super::{LazyPathListConfig, LazyPathListConfigFile, LazyStrategy::*};

    use crate::error::Result;

    #[tokio::test]
    async fn test_rule_match() -> Result<()> {
        let pathlist = r#"
        data/jpegs/e.jpg

        # this is a comment
            a.csv
        data/d.flac

        # below matches nothing
        *
        !
        "#;

        let mut cursor = Cursor::new(pathlist);

        let config = LazyPathListConfig::load(&mut cursor, false, POINTER, SMUDGE).await?;

        // paths that git filter spits out
        let queries_and_expected = vec![
            ("a.csv", SMUDGE),
            ("b.csv", POINTER),
            ("data/c.mp3", POINTER),
            ("data/d.flac", SMUDGE),
            ("data/jpegs/e.jpg", SMUDGE),
            ("data/jpegs/f.jpg", POINTER),
        ];

        for (q, e) in queries_and_expected {
            let matched_rule = config.match_rule(q);
            assert_eq!(matched_rule, e);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_config_changed_and_flush() -> Result<()> {
        let pathlist = ["a.csv", "data/d.flac", "data/jpegs/e.jpg"];

        // write the initial list into a temp file
        let tempd = tempfile::tempdir()?;
        let file_path = tempd.path().join("smudgelist");
        let mut buf = vec![];
        for p in pathlist {
            writeln!(&mut buf, "{p}")?;
        }
        std::fs::write(&file_path, buf)?;

        let mut config =
            LazyPathListConfigFile::load_smudge_list_from_file(&file_path, false).await?;

        // make some changes to the rules
        config.add_rules(&["b.csv"]);
        config.drop_rules(&["data/d.flac"]);

        // flush changes to file
        drop(config);

        // compare changes
        let expected_pathlist_after_change = vec!["a.csv", "b.csv", "data/jpegs/e.jpg"];

        let pathlist_from_file = std::fs::read_to_string(&file_path)?;
        let mut pathlist_from_file: Vec<_> = pathlist_from_file.split_whitespace().collect();
        pathlist_from_file.sort();

        assert_eq!(expected_pathlist_after_change, pathlist_from_file);

        Ok(())
    }
}
