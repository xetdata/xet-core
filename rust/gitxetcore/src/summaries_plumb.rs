use futures::prelude::stream::*;
use std::{
    collections::{hash_map::Entry, HashMap},
    fs::File,
    io::{BufWriter, Write},
    mem::swap,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use crate::{config::XetConfig, errors::GitXetRepoError, git_integration::GitXetRepo};
use crate::{
    constants::GIT_NOTES_SUMMARIES_REF_NAME, errors, git_integration::GitNotesWrapper,
    summaries::analysis::FileSummary,
};

const MAX_CONCURRENT_SUMMARY_MERGES: usize = 8;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct WholeRepoSummary {
    backing_file: PathBuf,
    // map of merkledb file hash to per-file summary
    dict: HashMap<String, FileSummary>,

    // modification flag
    #[serde(skip)]
    is_dirty: bool,
}

impl WholeRepoSummary {
    pub fn load(path: impl AsRef<Path>) -> Option<WholeRepoSummary> {
        // read db from file
        if let Ok(reader) = File::open(path) {
            let result: bincode::Result<WholeRepoSummary> = bincode::deserialize_from(reader);
            match result {
                Ok(db) => {
                    return Some(WholeRepoSummary {
                        backing_file: path.as_ref().to_path_buf(),
                        dict: db.dict,
                        is_dirty: false,
                    });
                }
                Err(e) => {
                    error!("Failed to load summary db: {}", e);
                }
            };
        }
        None
    }

    pub async fn load_or_recreate_from_git(
        repo: &Arc<GitXetRepo>,
        path: impl AsRef<Path>,
        notes_ref: &str,
    ) -> anyhow::Result<WholeRepoSummary> {
        if let Some(from_disk) = Self::load(path.as_ref()) {
            return Ok(from_disk);
        }

        let mut db = WholeRepoSummary::empty(path.as_ref());
        merge_db_from_git(repo, &mut db, notes_ref).await?;
        Ok(db)
    }

    pub fn empty(backing_file_path: impl AsRef<Path>) -> WholeRepoSummary {
        // create a new empty db
        WholeRepoSummary {
            backing_file: backing_file_path.as_ref().to_path_buf(),
            dict: Default::default(),
            is_dirty: false,
        }
    }

    // only available on &mut self, since otherwise it couldn't have changed since loading.
    pub fn flush(&mut self) -> anyhow::Result<()> {
        if !self.is_dirty {
            debug!("SummaryDB: skipping flush as no changes have been made.");
            return Ok(());
        }

        if self.backing_file == PathBuf::default() {
            return Ok(());
        }

        use std::io::{Error, ErrorKind};
        let dbpath = self.backing_file.parent().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Unable to find Summary db output parent path from {:?}",
                    self.backing_file
                ),
            )
        })?;

        let tempfile = tempfile::Builder::new()
            .prefix(&format!("{}.", std::process::id()))
            .suffix(".db")
            .tempfile_in(dbpath)?;

        debug!(
            "Flushing summary db to {:?} via {:?}",
            &self.backing_file,
            tempfile.path()
        );

        {
            let mut writer = BufWriter::new(&tempfile);
            bincode::serialize_into(&mut writer, &self)?;
            writer.flush()?;
        }

        tempfile.persist(&self.backing_file).map_err(|e| e.error)?;
        debug!("Done flushing summary db to {:?}", &self.backing_file);
        Ok(())
    }

    pub fn entry(&mut self, key: String) -> Entry<'_, String, FileSummary> {
        let entry = self.dict.entry(key);

        // If this operation has created a new entry, than the hash table is dirty
        if let Entry::Vacant(_) = &entry {
            self.is_dirty = true;
        }

        entry
    }

    pub fn merge_in(&mut self, mut other: Self) {
        if other.dict.len() > self.dict.len() {
            swap(&mut other.dict, &mut self.dict);
            self.is_dirty = true;
        }

        for (k, v) in other.dict {
            if let Entry::Vacant(e) = self.entry(k) {
                e.insert(v);
            }
        }
    }

    fn iter(&self) -> std::collections::hash_map::Iter<String, FileSummary> {
        self.dict.iter()
    }

    pub fn get(&self, key: &str) -> Option<&FileSummary> {
        self.dict.get(key)
    }

    fn insert(&mut self, key: String, value: FileSummary) {
        if let Entry::Vacant(e) = self.entry(key) {
            e.insert(value);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.dict.is_empty()
    }
}

/// Aggregates all the summary dbs stored in git notes into a single DB
async fn merge_db_from_git(
    repo: &Arc<GitXetRepo>,
    db: &mut WholeRepoSummary,
    notesref: &str,
) -> anyhow::Result<()> {
    let repo_notes = GitNotesWrapper::open(repo.git_repo().clone(), notesref);

    let mut blob_strm = iter(
        repo_notes.notes_content_iterator()
.map_err(|e| {
        error!("merge_db_from_git: Unable to iterate over notes at {notesref:?}: {e:?}");
        e
    })?
            .map(|(_, blob)| async move {
                if !blob.is_empty() {
                    bincode::deserialize::<WholeRepoSummary>(&blob).map_err(|e| {
                        info!("Error unpacking file content summary information; discarding (Error = {:?})", &e);
                        e
                    }).unwrap_or_default()
                } else {
                    WholeRepoSummary::default()
                }
            }),
    )
    .buffered(MAX_CONCURRENT_SUMMARY_MERGES);

    while let Some(notes_db) = blob_strm.next().await {
        db.merge_in(notes_db);
    }

    Ok(())
}

/// Aggregates all the summaries stored in git notes into a single struct
pub async fn merge_summaries_from_git(
    repo: &Arc<GitXetRepo>,
    output: &Path,
    notes_ref: &str,
) -> anyhow::Result<()> {
    let mut db = WholeRepoSummary::load(output).unwrap_or_else(|| WholeRepoSummary::empty(output));
    merge_db_from_git(repo, &mut db, notes_ref).await?;
    db.flush()
}

pub fn encode_summary_db_to_note(summarydb: &WholeRepoSummary) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serialize(summarydb)?)
}

pub async fn update_summaries_to_git(
    repo: &Arc<GitXetRepo>,
    input: &Path,
    notesref: &str,
) -> Result<(), GitXetRepoError> {
    // open the input db
    let inputdb = WholeRepoSummary::load_or_recreate_from_git(repo, input, notesref).await?;

    // figure out the db contents of git
    let mut gitdb = WholeRepoSummary::empty(input);
    merge_db_from_git(repo, &mut gitdb, notesref).await?;

    // calculate the diff
    let diffdb = whole_repo_summary_difference(&inputdb, &gitdb);

    // save some memory
    drop(inputdb);
    drop(gitdb);

    // serialize the diff into memory
    let vec = encode_summary_db_to_note(&diffdb)?;
    drop(diffdb);

    let repo_notes = GitNotesWrapper::open(repo.git_repo().clone(), notesref);

    repo_notes.add_note(vec).map_err(|e| {
        error!("update_summaries_to_git: Error inserting new note in update_summaries_to_git ({notesref:?}): {e:?}");
        e
    })?;

    Ok(())
}

pub fn whole_repo_summary_difference(
    inputdb: &WholeRepoSummary,
    gitdb: &WholeRepoSummary,
) -> WholeRepoSummary {
    let mut diff = WholeRepoSummary::empty(&inputdb.backing_file);

    // for this method, inputdb is considered the source of truth.
    for (key, input_value) in inputdb.iter() {
        if let Some(gitdb_value) = gitdb.get(key) {
            // only create key in diff if they differ
            if let Some(diff_value) = input_value.diff(gitdb_value) {
                diff.insert(key.clone(), diff_value);
            }
        } else {
            // not found in gitdb, copy the whole value
            diff.insert(key.clone(), input_value.clone());
        }
    }

    diff
}

// Lists the summary contents of git notes, writing to stdout
pub async fn summaries_list_git(config: XetConfig) -> errors::Result<()> {
    let repo = GitXetRepo::open(config)?;
    let db = WholeRepoSummary::load_or_recreate_from_git(
        &repo,
        &repo.config().summarydb,
        GIT_NOTES_SUMMARIES_REF_NAME,
    )
    .await?;
    if db.is_empty() {
        warn!("No summary entries found.");
        return Ok(());
    }
    println!("id, types");
    for (k, v) in db.iter() {
        println!("{}, {}", k, v.list_types());
    }
    Ok(())
}

// Queries for the stats summary for a file with the merklehash
pub async fn summaries_query(config: XetConfig, merklehash: &str) -> errors::Result<()> {
    let repo = GitXetRepo::open(config)?;
    let db = WholeRepoSummary::load_or_recreate_from_git(
        &repo,
        &repo.config().summarydb,
        GIT_NOTES_SUMMARIES_REF_NAME,
    )
    .await?;
    let summary = db.get(merklehash);
    let json = serde_json::to_string_pretty(&summary)?;
    println!("{json}");
    Ok(())
}

// Writes out all the summary jsons to stdout
pub async fn summaries_dump(config: XetConfig) -> errors::Result<()> {
    let repo = GitXetRepo::open(config)?;
    let db = WholeRepoSummary::load_or_recreate_from_git(
        &repo,
        &repo.config().summarydb,
        GIT_NOTES_SUMMARIES_REF_NAME,
    )
    .await?;
    let json = serde_json::to_string_pretty(&db)?;
    println!("{json}");
    Ok(())
}
