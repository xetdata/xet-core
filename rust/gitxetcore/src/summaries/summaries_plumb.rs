use futures::prelude::stream::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::Entry, HashMap},
    fs::File,
    io::{BufWriter, Write},
    mem::swap,
    path::{Path, PathBuf},
};
use tracing::{debug, error, info, warn};

use crate::{config::XetConfig, errors::GitXetRepoError};
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
    pub fn load(path: &Path) -> Option<WholeRepoSummary> {
        // read db from file
        if let Ok(reader) = File::open(path) {
            let result: bincode::Result<WholeRepoSummary> = bincode::deserialize_from(reader);
            match result {
                Ok(db) => {
                    return Some(WholeRepoSummary {
                        backing_file: path.to_path_buf(),
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
        config: &XetConfig,
        path: &Path,
        notes_ref: &str,
    ) -> anyhow::Result<WholeRepoSummary> {
        if let Some(from_disk) = Self::load(path) {
            return Ok(from_disk);
        }

        let mut db = WholeRepoSummary::empty(path);
        merge_db_from_git(config, &mut db, notes_ref).await?;
        Ok(db)
    }

    pub fn empty(backing_file_path: &Path) -> WholeRepoSummary {
        // create a new empty db
        WholeRepoSummary {
            backing_file: backing_file_path.to_path_buf(),
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
    config: &XetConfig,
    db: &mut WholeRepoSummary,
    notesref: &str,
) -> anyhow::Result<()> {
    let repo =
        GitNotesWrapper::open(config.get_implied_repo_path()?, config, notesref).map_err(|e| {
            error!("merge_db_from_git: Unable to access git notes at {notesref:?}: {e:?}");
            e
        })?;

    let mut blob_strm = iter(
        repo.notes_content_iterator()
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
    config: &XetConfig,
    output: &Path,
    notes_ref: &str,
) -> anyhow::Result<()> {
    let mut db = WholeRepoSummary::load(output).unwrap_or_else(|| WholeRepoSummary::empty(output));
    merge_db_from_git(config, &mut db, notes_ref).await?;
    db.flush()
}

pub fn encode_summary_db_to_note(summarydb: &WholeRepoSummary) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serialize(summarydb)?)
}

pub async fn update_summaries_to_git(
    config: &XetConfig,
    input: &Path,
    notesref: &str,
) -> Result<(), GitXetRepoError> {
    // open the input db
    let inputdb = WholeRepoSummary::load_or_recreate_from_git(config, input, notesref).await?;

    // figure out the db contents of git
    let mut gitdb = WholeRepoSummary::empty(input);
    merge_db_from_git(config, &mut gitdb, notesref).await?;

    // calculate the diff
    let diffdb = whole_repo_summary_difference(&inputdb, &gitdb);

    // save some memory
    drop(inputdb);
    drop(gitdb);

    // serialize the diff into memory
    let vec = encode_summary_db_to_note(&diffdb)?;
    drop(diffdb);

    let repo =
        GitNotesWrapper::open(config.get_implied_repo_path()?, config, notesref).map_err(|e| {
            error!("update_summaries_to_git: Unable to access git notes at {notesref:?}: {e:?}");
            e
        })?;

    repo.add_note(vec).map_err(|e| {
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
    let db = WholeRepoSummary::load_or_recreate_from_git(
        &config,
        &config.summarydb,
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
    let db = WholeRepoSummary::load_or_recreate_from_git(
        &config,
        &config.summarydb,
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
    let db = WholeRepoSummary::load_or_recreate_from_git(
        &config,
        &config.summarydb,
        GIT_NOTES_SUMMARIES_REF_NAME,
    )
    .await?;
    let json = serde_json::to_string_pretty(&db)?;
    println!("{json}");
    Ok(())
}

/// Tests the compatibility of differently serialized versions of the [WholeRepoSummary]
/// This expects different versions of the codebase and the older files.
/// Files can be generated from different versions (v0 <=0.13.3) or found in:
/// https://www.notion.so/xethub/Summary-Versioning-Compatibility-21f173305a9c44dea05c33ed5b176546?pvs=4#4c2f4928aa634c249113c6727422f8bf
#[cfg(test)]
mod test_serde {
    use super::*;
    use crate::summaries::analysis::{SummaryExt, ADDITIONAL_SUMMARY_VERSION};
    use crate::summaries::csv::CSVSummary;
    use crate::summaries::summarize_csv_from_reader;
    use std::borrow::Cow;
    use std::env;
    use tableau_summary::twb::printer::summarize_twb_from_reader;
    use tableau_summary::twb::TwbSummary;

    const BIN_SUF: &str = ".bin";
    // const JSON_SUF: &str = ".json";
    const V0_SUF: &str = ".v0"; // TwbSummary directly
    const V1_SUF: &str = ".v1"; // Versioner V1
    const V2_SUF: &str = ".v2"; // Versioner V2

    const CSV_PATH: &str = "tests/data/file.csv";
    const TWB_PATH: &str = "tests/data/workbook.twb";

    const CSV_DB: &str = "tests/data/single_csv";
    const TWB_DB: &str = "tests/data/single_twb";
    const BOTH_DB: &str = "tests/data/both";

    #[test]
    #[ignore = "v0"]
    fn test_summarize_v0() {
        env::set_var("XET_CSV_MIN_SIZE", "10");
        let csv_summary = summarize_csv(CSV_PATH);
        let summary_map = vec![(CSV_PATH.to_string(), csv_summary.clone())]
            .into_iter()
            .collect();

        serialize_summary(CSV_DB, V0_SUF, summary_map);

        let twb_summary = summarize_twb(TWB_PATH);
        let summary_map = vec![(TWB_PATH.to_string(), twb_summary.clone())]
            .into_iter()
            .collect();

        serialize_summary(TWB_DB, V0_SUF, summary_map);

        let summary_map = vec![
            (CSV_PATH.to_string(), csv_summary),
            (TWB_PATH.to_string(), twb_summary),
        ]
        .into_iter()
        .collect();
        serialize_summary(BOTH_DB, V0_SUF, summary_map);
    }

    #[test]
    #[ignore = "v1"]
    fn test_summarize_v1() {
        env::set_var("XET_CSV_MIN_SIZE", "10");
        let csv_summary = summarize_csv(CSV_PATH);
        let summary_map = vec![(CSV_PATH.to_string(), csv_summary.clone())]
            .into_iter()
            .collect();

        serialize_summary(CSV_DB, V1_SUF, summary_map);

        let twb_summary = summarize_twb(TWB_PATH);
        let summary_map = vec![(TWB_PATH.to_string(), twb_summary.clone())]
            .into_iter()
            .collect();

        serialize_summary(TWB_DB, V1_SUF, summary_map);

        let summary_map = vec![
            (CSV_PATH.to_string(), csv_summary),
            (TWB_PATH.to_string(), twb_summary),
        ]
        .into_iter()
        .collect();
        serialize_summary(BOTH_DB, V1_SUF, summary_map);
    }

    #[test]
    #[ignore = "v2"]
    fn test_summarize_v2() {
        env::set_var("XET_CSV_MIN_SIZE", "10");
        let csv_summary = summarize_csv(CSV_PATH);
        let summary_map = vec![(CSV_PATH.to_string(), csv_summary.clone())]
            .into_iter()
            .collect();

        serialize_summary(CSV_DB, V2_SUF, summary_map);

        let twb_summary = summarize_twb(TWB_PATH);
        let summary_map = vec![(TWB_PATH.to_string(), twb_summary.clone())]
            .into_iter()
            .collect();

        serialize_summary(TWB_DB, V2_SUF, summary_map);

        let summary_map = vec![
            (CSV_PATH.to_string(), csv_summary),
            (TWB_PATH.to_string(), twb_summary),
        ]
        .into_iter()
        .collect();
        serialize_summary(BOTH_DB, V2_SUF, summary_map);
    }

    #[test]
    #[ignore = "v0"]
    fn test_deserialize_v0() {
        let path = format!("{CSV_DB}{V0_SUF}{BIN_SUF}");
        let db = WholeRepoSummary::load(PathBuf::from(path).as_path()).unwrap();
        let some_csv = get_csv_summary(&db, CSV_PATH);
        assert!(some_csv.is_some());

        let path = format!("{TWB_DB}{V0_SUF}{BIN_SUF}");
        let db = WholeRepoSummary::load(PathBuf::from(path).as_path()).unwrap();
        let some_twb = get_twb_summary(&db, TWB_PATH);
        assert!(some_twb.is_some());

        let path = format!("{BOTH_DB}{V0_SUF}{BIN_SUF}");
        let db = WholeRepoSummary::load(PathBuf::from(path).as_path()).unwrap();
        let some_csv = get_csv_summary(&db, CSV_PATH);
        assert!(some_csv.is_some());
        let some_twb = get_twb_summary(&db, TWB_PATH);
        assert!(some_twb.is_some());
    }

    #[test]
    #[ignore = "v1"]
    fn test_deserialize_v1() {
        let path = format!("{CSV_DB}{V1_SUF}{BIN_SUF}");
        let db = WholeRepoSummary::load(PathBuf::from(path).as_path()).unwrap();
        let some_csv = get_csv_summary(&db, CSV_PATH);
        assert!(some_csv.is_some());

        let path = format!("{TWB_DB}{V1_SUF}{BIN_SUF}");
        let db = WholeRepoSummary::load(PathBuf::from(path).as_path()).unwrap();
        let some_twb = get_twb_summary(&db, TWB_PATH);
        assert!(some_twb.is_some());

        let path = format!("{BOTH_DB}{V1_SUF}{BIN_SUF}");
        let db = WholeRepoSummary::load(PathBuf::from(path).as_path()).unwrap();
        let some_csv = get_csv_summary(&db, CSV_PATH);
        assert!(some_csv.is_some());
        let some_twb = get_twb_summary(&db, TWB_PATH);
        assert!(some_twb.is_some());
    }

    #[test]
    #[ignore = "v2"]
    fn test_deserialize_v2() {
        let path = format!("{CSV_DB}{V2_SUF}{BIN_SUF}");
        let db = WholeRepoSummary::load(PathBuf::from(path).as_path()).unwrap();
        let some_csv = get_csv_summary(&db, CSV_PATH);
        assert!(some_csv.is_some());

        let path = format!("{TWB_DB}{V2_SUF}{BIN_SUF}");
        let db = WholeRepoSummary::load(PathBuf::from(path).as_path()).unwrap();
        let some_twb = get_twb_summary(&db, TWB_PATH);
        assert!(some_twb.is_some());

        let path = format!("{BOTH_DB}{V2_SUF}{BIN_SUF}");
        let db = WholeRepoSummary::load(PathBuf::from(path).as_path()).unwrap();
        let some_csv = get_csv_summary(&db, CSV_PATH);
        assert!(some_csv.is_some());
        let some_twb = get_twb_summary(&db, TWB_PATH);
        assert!(some_twb.is_some());
    }

    fn serialize_summary(name: &str, version: &str, summary_map: HashMap<String, FileSummary>) {
        let path = PathBuf::from(format!("{name}{version}{BIN_SUF}"));
        let db = WholeRepoSummary {
            backing_file: path.clone(),
            dict: summary_map,
            is_dirty: false,
        };
        let file = File::create(&path).unwrap();
        let mut writer = BufWriter::new(file);
        bincode::serialize_into(&mut writer, &db).unwrap();
        writer.flush().unwrap();
    }

    fn summarize_csv(file_path: &str) -> FileSummary {
        let mut data = File::open(file_path).unwrap();
        let summary = summarize_csv_from_reader(&mut data, b',').unwrap();
        assert!(summary.is_some());
        FileSummary {
            csv: summary,
            libmagic: None,
            additional_summaries: None,
        }
    }

    fn summarize_twb(file_path: &str) -> FileSummary {
        let mut data = File::open(file_path).unwrap();
        let summary = summarize_twb_from_reader(&mut data).unwrap();
        assert!(summary.is_some());
        FileSummary {
            csv: None,
            libmagic: None,
            additional_summaries: Some(SummaryExt {
                version: ADDITIONAL_SUMMARY_VERSION,
                twb: summary,
                tds: None,
            }),
        }
    }

    fn get_csv_summary<'a>(db: &'a WholeRepoSummary, key: &str) -> Option<&'a CSVSummary> {
        db.dict.get(&key.to_string()).and_then(|s| s.csv.as_ref())
    }

    fn get_twb_summary<'a>(db: &'a WholeRepoSummary, key: &str) -> Option<Cow<'a, TwbSummary>> {
        db.dict
            .get(&key.to_string())
            .and_then(|s| s.additional_summaries.as_ref())
            .and_then(|ext| ext.twb.as_ref())
            .and_then(TwbSummary::from_ref)
    }
}
