use crate::config::XetConfig;
use crate::constants::MAX_CONCURRENT_UPLOADS;
use crate::data_processing::create_cas_client;
use crate::errors;
use crate::errors::GitXetRepoError;
use crate::git_integration::git_notes_wrapper::GitNotesWrapper;
use crate::git_integration::git_repo::get_merkledb_notes_name;
use crate::merkledb_plumb::*;
use crate::utils::*;
use parutils::tokio_par_for_each;
use shard_client::{GrpcShardClient, RegistrationClient, ShardConnectionConfig};

use anyhow::Context;
use bincode::Options;
use cas_client::Staging;
use git2::Oid;
use mdb_shard::merging::consolidate_shards_in_directory;
use mdb_shard::shard_file::MDBShardFileFooter;
use mdb_shard::shard_file::MDBShardInfo;
use mdb_shard::shard_file::MDB_SHARD_MIN_TARGET_SIZE;
use mdb_shard::shard_file_manager::ShardFileManager;
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use mdb_shard::shard_handle::MDBShardFile;
use mdb_shard::shard_version::ShardVersion;
use merkledb::MerkleMemDB;
use merklehash::{HashedWrite, MerkleHash};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use std::{
    collections::HashSet,
    fs,
    io::{BufReader, BufWriter, Cursor, Read, Write},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    vec,
};
use tempfile::TempDir;
use tracing::error;
use tracing::{debug, info};

const MERKLEDB_NOTES_ENCODING_VERSION_2: u64 = 2;
const MDB_SHARD_META_ENCODING_VERSION: u64 = 0;
const MDB_SHARD_META_COLLECTION_HEADER_SIZE: usize = 8;
const GIT_OID_RAWSZ: usize = 20;

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
struct MDBShardMeta {
    shard_hash: MerkleHash,
    shard_footer: MDBShardFileFooter,
    converted_v1_notes_head: Option<[u8; GIT_OID_RAWSZ]>,
}

impl MDBShardMeta {
    fn new(shard_hash: &MerkleHash, converted_v1_notes_head: Option<Oid>) -> Self {
        Self {
            shard_hash: *shard_hash,
            converted_v1_notes_head: if let Some(oid) = converted_v1_notes_head {
                let mut bytes = [0u8; GIT_OID_RAWSZ];
                bytes.copy_from_slice(oid.as_bytes());
                Some(bytes)
            } else {
                None
            },
            ..Default::default()
        }
    }

    fn decode(reader: impl Read) -> errors::Result<Self> {
        let options = bincode::DefaultOptions::new().with_fixint_encoding();
        options.deserialize_from(reader).map_err(|_| {
            errors::GitXetRepoError::DataParsingError("Unable to deserialize a MDBShardMeta".into())
        })
    }

    fn encode(&self, write: impl Write) -> errors::Result<()> {
        let options = bincode::DefaultOptions::new().with_fixint_encoding();
        options.serialize_into(write, self).map_err(|_| {
            errors::GitXetRepoError::DataParsingError("Unable to serialize a MDBShardMeta".into())
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct MDBShardMetaCollectionHeader {
    version: u64,
}

impl MDBShardMetaCollectionHeader {
    fn new(version: u64) -> Self {
        Self { version }
    }

    fn decode(reader: impl Read) -> errors::Result<Self> {
        let options = bincode::DefaultOptions::new().with_fixint_encoding();
        options.deserialize_from(reader).map_err(|_| {
            errors::GitXetRepoError::DataParsingError("Unable to deserialize a MDBShardMeta".into())
        })
    }

    fn encode(&self, write: impl Write) -> errors::Result<()> {
        let options = bincode::DefaultOptions::new().with_fixint_encoding();
        options.serialize_into(write, self).map_err(|_| {
            errors::GitXetRepoError::DataParsingError("Unable to serialize a MDBShardMeta".into())
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
struct MDBShardMetaCollection {
    shard_metas: Vec<MDBShardMeta>,
}

impl Deref for MDBShardMetaCollection {
    type Target = Vec<MDBShardMeta>;

    fn deref(&self) -> &Self::Target {
        self.shard_metas.as_ref()
    }
}

impl DerefMut for MDBShardMetaCollection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.shard_metas.as_mut()
    }
}

impl MDBShardMetaCollection {
    fn open(path: &Path) -> errors::Result<MDBShardMetaCollection> {
        let reader = BufReader::new(fs::File::open(path)?);
        MDBShardMetaCollection::decode(reader)
    }

    fn decode(reader: impl Read) -> errors::Result<MDBShardMetaCollection> {
        let options = bincode::DefaultOptions::new().with_fixint_encoding();
        options.deserialize_from(reader).map_err(|_| {
            errors::GitXetRepoError::DataParsingError(
                "Unable to deserialize a MDBShardMetaCollection".into(),
            )
        })
    }

    fn encode(&self, writer: impl Write) -> errors::Result<()> {
        let options = bincode::DefaultOptions::new().with_fixint_encoding();
        options.serialize_into(writer, &self).map_err(|_| {
            errors::GitXetRepoError::DataParsingError(
                "Unable to serialize a MDBShardMetaCollection".into(),
            )
        })
    }

    fn flush(self, path: &Path) -> errors::Result<()> {
        let mut writer = Cursor::new(Vec::<u8>::new());

        self.encode(&mut writer)?;

        write_all_file_safe(path, &writer.into_inner())?;

        Ok(())
    }
}

impl IntoIterator for MDBShardMetaCollection {
    type Item = MDBShardMeta;
    type IntoIter = std::vec::IntoIter<MDBShardMeta>;

    fn into_iter(self) -> Self::IntoIter {
        self.shard_metas.into_iter()
    }
}

/// Encode a collection of MDBShardMeta to a note entry.
fn encode_shard_meta_collection_to_note(
    collection: &MDBShardMetaCollection,
) -> errors::Result<Vec<u8>> {
    let mut buffer = Cursor::new(Vec::new());
    MerkleDBNotesHeader::new(MERKLEDB_NOTES_ENCODING_VERSION_2).encode(&mut buffer)?;
    MDBShardMetaCollectionHeader::new(MDB_SHARD_META_ENCODING_VERSION).encode(&mut buffer)?;
    collection.encode(&mut buffer)?;
    Ok(buffer.into_inner())
}

/// Decode a collection of MDBShardMeta from a note entry.
fn decode_shard_meta_collection_from_note(blob: &[u8]) -> errors::Result<MDBShardMetaCollection> {
    let header = MerkleDBNotesHeader::decode(blob)?;
    debug!("Parsed a MDB header {:?}", header);
    let version = header.get_version();
    if version == MERKLEDB_NOTES_ENCODING_VERSION_2 {
        let remaining_bytes = &blob[MERKLEDB_NOTES_HEADER_SIZE..];

        let collection_header = MDBShardMetaCollectionHeader::decode(remaining_bytes)?;
        let collection_version = collection_header.version;

        if collection_version == 0 {
            let remaining_bytes = &remaining_bytes[MDB_SHARD_META_COLLECTION_HEADER_SIZE..];
            MDBShardMetaCollection::decode(remaining_bytes)
        } else {
            panic!("Encoutering version {collection_version} of MerkleDB shard meta format. Please upgrade git-xet");
        }
    } else {
        panic!("Encountering version {version} of MerkleDB format. Please upgrade git-xet");
    }
}

/// Download MDB Shards from CAS if not present in the output dir,
/// convert MDB v1 if there is update in ref notes.
pub async fn sync_mdb_shards_from_git(
    config: &XetConfig,
    cache_dir: &Path,
    notesref_v2: &str,
    fetch_all_shards: bool,
) -> errors::Result<()> {
    let cache_meta = get_cache_meta_file(cache_dir)?;
    let cache_head = get_cache_head_file(cache_dir)?;

    if let Some(head) =
        sync_mdb_shards_meta_from_git(config, &cache_meta, &cache_head, notesref_v2)?
    {
        if fetch_all_shards {
            sync_mdb_shards_from_cas(config, &cache_meta, cache_dir).await?;
        }
        write_all_file_safe(&cache_head, head.as_bytes())?;
    }

    Ok(())
}

/// A serialized file that contains meta data (including footer)
/// of all MDB shards in ref notes.
pub fn get_cache_meta_file(cache_dir: &Path) -> errors::Result<PathBuf> {
    Ok(cache_dir.to_owned().with_extension("meta"))
}

/// This file keeps track of the HEAD of MDB shard ref notes
/// that the 'cache meta' file is obtained from. If this matches
/// the current ref notes HEAD we don't need to walk the ref notes.
pub fn get_cache_head_file(cache_dir: &Path) -> errors::Result<PathBuf> {
    Ok(cache_dir.to_owned().with_extension("HEAD"))
}

/// Sync MDB v2 ref notes to cache_meta, skip if cache_head matches HEAD of the ref notes.
/// Return ref notes HEAD if pulling updates from ref notes.
fn sync_mdb_shards_meta_from_git(
    config: &XetConfig,
    cache_meta: &Path,
    cache_head: &Path,
    notesref: &str,
) -> errors::Result<Option<Oid>> {
    info!("Sync shards meta from git");
    let ref_notes_head = ref_to_oid(config, notesref)?;

    if ref_notes_head.is_none() {
        return Ok(None);
    }

    // If any of the the two file doesn't exist, we are
    // out of sync and should pull from ref notes.
    if cache_head.exists() && cache_meta.exists() {
        let cache_head = Oid::from_bytes(&fs::read(cache_head)?).ok();

        // cache is up to date, no need to sync
        if ref_notes_head == cache_head {
            return Ok(None);
        }
    }

    let mut shard_metas = MDBShardMetaCollection::default();

    // Walk the ref notes tree and deserialize all into a collection.
    let repo = GitNotesWrapper::open(get_repo_path_from_config(config)?, notesref)
        .with_context(|| format!("Unable to access git notes at {notesref:?}"))?;

    for oid in repo
        .notes_name_iterator()
        .with_context(|| format!("Unable to iterate over git notes at {notesref:?}"))?
    {
        if let Ok(blob) = repo.notes_name_to_content(&oid) {
            let collection = decode_shard_meta_collection_from_note(&blob).with_context(|| {
                format!("Unable to parse notes entry to MDBShardMetaCollection at {oid}")
            })?;

            debug!("Parsed a MetaCollection: {collection:?}");

            shard_metas.extend(collection);
        }
    }

    shard_metas.flush(cache_meta)?;

    Ok(ref_notes_head)
}

/// Sync MDB shards to cache_dir, skip if shard already exists in this directory.
async fn sync_mdb_shards_from_cas(
    config: &XetConfig,
    cache_meta: &Path,
    cache_dir: &Path,
) -> errors::Result<()> {
    info!("Sync shards from CAS");
    let cas = create_cas_client(config).await?;

    let metas = MDBShardMetaCollection::open(cache_meta)?;

    // TODO: run in parallel after passing tests.
    for meta in metas {
        let shard_name = cache_dir.join(local_shard_name(&meta.shard_hash));
        if shard_name.exists() {
            debug!("sync_mdb_shards_from_cas: shard file {shard_name:?} exists.");
            continue;
        } else {
            debug!("sync_mdb_shards_from_cas: shard file {shard_name:?} does not exist, downloading from cas.");
        }

        download_shard(config, &cas, &meta.shard_hash, cache_dir).await?;
    }

    Ok(())
}

#[allow(clippy::borrowed_box)]
pub async fn download_shard(
    config: &XetConfig,
    cas: &Arc<dyn Staging + Send + Sync>,
    shard_hash: &MerkleHash,
    dest_dir: &Path,
) -> errors::Result<PathBuf> {
    let prefix = config.cas.shard_prefix();

    let bytes: Vec<u8> = match cas.get(&prefix, shard_hash).await {
        Err(e) => {
            error!("Error attempting to download shard {prefix}/{shard_hash:?}: {e:?}");
            Err(e)?
        }
        Ok(data) => data,
    };

    info!("Downloaded shard {prefix}/{shard_hash:?}.");

    let dest_file = dest_dir.join(local_shard_name(shard_hash));

    write_all_file_safe(&dest_file, &bytes)?;

    Ok(dest_file)
}

/// Check if should convert MDB v1 shards.
/// Returns true if the current MDB v1 ref notes HEAD doesn't appear in
/// any MDB v2 ref notes conversion history.
#[allow(dead_code)]
fn should_upgrade_from_v1(
    config: &XetConfig,
    cache_meta: &Path,
    notesref: &str,
) -> errors::Result<bool> {
    let ref_notes_head = ref_to_oid(config, notesref)?;

    if ref_notes_head.is_none() {
        return Ok(false);
    }

    let mut converted_v1_heads_list = HashSet::<Oid>::new();

    let metas = MDBShardMetaCollection::open(cache_meta)?;

    for meta in metas {
        if let Some(oid) = meta.converted_v1_notes_head {
            converted_v1_heads_list.insert(Oid::from_bytes(&oid)?);
        }
    }

    Ok(!converted_v1_heads_list.contains(&ref_notes_head.unwrap()))
}

/// Remove existing MDB shards in dir that is a conversion
/// from MDB v1. Also remove the corresponding meta files.
#[allow(dead_code)]
fn clean_existing_v1_conversions(dir: &Path) -> errors::Result<()> {
    for meta_file in fs::read_dir(dir)?
        .flatten()
        .filter(|f| is_meta_file(&f.path()))
    {
        fs::remove_file(meta_file.path())?;
        fs::remove_file(meta_to_shard(&meta_file.path()))?;
    }

    Ok(())
}

/// Merge Merkledb from notes and upgrade to MDB Shard,
/// write a conversion meta along with the shard.
/// Return the meta of the result shard.
#[allow(dead_code)]
async fn upgrade_from_v1(
    config: &XetConfig,
    output: &Path,
    notesref: &str,
) -> errors::Result<MDBShardMeta> {
    let mut dbv1 = MerkleMemDB::default();
    merge_db_from_git(config, &mut dbv1, notesref).await?;

    let shard_hash = convert_merklememdb(output, &dbv1)?;

    let shard_meta = MDBShardMeta::new(&shard_hash, ref_to_oid(config, notesref)?);

    // Save meta into a file under output dir.
    let mut buffer = Cursor::new(Vec::<u8>::new());
    shard_meta.encode(&mut buffer)?;

    write_all_file_safe(
        output.join(local_meta_name(&shard_hash)).as_path(),
        &buffer.into_inner(),
    )?;

    Ok(shard_meta)
}

/// Convert a Merkle DB v1 to a MDB shard.
/// Return the hash of the result shard.
#[allow(dead_code)]
fn convert_merklememdb(output: &Path, db: &MerkleMemDB) -> errors::Result<MerkleHash> {
    let tempfile = create_temp_file(output, "mdb")?;

    let mut hashed_write = HashedWrite::new(&tempfile);

    {
        let mut buf_write = BufWriter::new(&mut hashed_write);
        MDBShardInfo::serialize_from_v1(&mut buf_write, db)?;
    }

    hashed_write.flush()?;

    let shard_hash = hashed_write.hash();

    tempfile
        .persist(output.join(local_shard_name(&shard_hash)))
        .map_err(|e| e.error)?;

    Ok(shard_hash)
}

/// Consolidate shards from sessions, install guard into ref notes v1 if
/// a conversion was done. Write shards into ref notes v2.
pub async fn sync_mdb_shards_to_git(
    config: &XetConfig,
    session_dir: &Path,
    cache_dir: &Path,
    notesref_v2: &str,
) -> errors::Result<()> {
    let merged_shards = consolidate_shards_in_directory(session_dir, MDB_SHARD_MIN_TARGET_SIZE)?;

    sync_session_shards_to_remote(config, merged_shards).await?;

    // Write v2 ref notes.
    update_mdb_shards_to_git_notes(config, session_dir, notesref_v2)?;

    move_session_shards_to_local_cache(session_dir, cache_dir).await?;

    Ok(())
}

pub async fn sync_session_shards_to_remote(
    config: &XetConfig,
    shards: Vec<MDBShardFile>,
) -> errors::Result<()> {
    // Consolidate all the shards.

    if !shards.is_empty() {
        let cas = create_cas_client(config).await?;
        let cas_ref = &cas;

        let (user_id, _) = config.user.get_user_id();

        // For now, got the config stuff working.
        let shard_connection_config = ShardConnectionConfig {
            endpoint: config.cas.endpoint.clone(),
            user_id,
            git_xet_version: crate::data_processing_v2::GIT_XET_VERION.to_string(),
        };

        let shard_file_client = {
            if config.cas.endpoint.starts_with("local://") {
                None
            } else {
                Some(GrpcShardClient::from_config(shard_connection_config).await?)
            }
        };

        let shard_file_client_ref = shard_file_client.as_ref();
        let shard_prefix = config.cas.shard_prefix();
        let shard_prefix_ref = &shard_prefix;

        tokio_par_for_each(shards, MAX_CONCURRENT_UPLOADS, |si, _| async move {
            // For each shard:
            // 1. Upload directly to CAS.
            // 2. Sync to server.

            info!(
                "Uploading shard {shard_prefix_ref}/{:?} from staging area to CAS.",
                &si.shard_hash
            );
            let data = fs::read(&si.path)?;
            let data_len = data.len();
            // Upload the shard.
            cas_ref
                .put_bypass_stage(
                    shard_prefix_ref,
                    &si.shard_hash,
                    data,
                    vec![data_len as u64],
                )
                .await?;

            info!(
                "Registering shard {shard_prefix_ref}/{:?} with shard server.",
                &si.shard_hash
            );

            // That succeeded if we made it here, so now try to sync things.
            if let Some(sfc) = shard_file_client_ref {
                sfc.register_shard(shard_prefix_ref, &si.shard_hash).await?;

                info!(
                    "Shard {shard_prefix_ref}/{:?} upload + sync successful.",
                    &si.shard_hash
                );
            } else {
                info!(
                    "Shard {shard_prefix_ref}/{:?} sent to local CAS; sync skipped",
                    &si.shard_hash
                );
            }

            Ok(())
        })
        .await
        .map_err(|e| match e {
            parutils::ParallelError::JoinError => {
                GitXetRepoError::InternalError(anyhow::anyhow!("Join Error"))
            }
            parutils::ParallelError::TaskError(e) => e,
        })?;
    }
    Ok(())
}

pub fn create_new_mdb_shard_note(session_dir: &Path) -> errors::Result<Option<Vec<u8>>> {
    let dir_walker = fs::read_dir(session_dir)?;

    let mut collection = MDBShardMetaCollection::default();

    for file in dir_walker.flatten() {
        let file_type = file.file_type()?;
        let file_path = file.path();
        if !file_type.is_file() || !is_shard_file(&file_path) {
            continue;
        }

        let meta_file = shard_to_meta(&file_path);
        let shard_meta = match meta_file.exists() {
            true => MDBShardMeta::decode(fs::File::open(meta_file)?)?,
            false => {
                let mut meta = MDBShardMeta::new(
                    &shard_path_to_hash(&file_path).map_err(|_| {
                        GitXetRepoError::DataParsingError(format!(
                            "Cannot parse hash for path {}",
                            file_path.display()
                        ))
                    })?,
                    None,
                );

                let mut reader = fs::File::open(&file_path)?;
                let shard_info = MDBShardInfo::load_from_file(&mut reader)?;
                meta.shard_footer = shard_info.metadata;

                meta
            }
        };

        collection.push(shard_meta);
    }

    if collection.is_empty() {
        Ok(None)
    } else {
        Ok(Some(encode_shard_meta_collection_to_note(&collection)?))
    }
}

fn update_mdb_shards_to_git_notes(
    config: &XetConfig,
    session_dir: &Path,
    notesref: &str,
) -> errors::Result<()> {
    let repo = GitNotesWrapper::open(get_repo_path_from_config(config)?, notesref)
        .with_context(|| format!("Unable to access git notes at {notesref:?}"))?;

    if let Some(shard_note_data) = create_new_mdb_shard_note(session_dir)? {
        repo.add_note(shard_note_data)
            .with_context(|| "Unable to insert new note")?;
    }

    Ok(())
}

pub async fn move_session_shards_to_local_cache(
    session_dir: &Path,
    cache_dir: &Path,
) -> errors::Result<()> {
    let dir_walker = fs::read_dir(session_dir)?;

    for file in dir_walker.flatten() {
        let file_type = file.file_type()?;
        let file_path = file.path();
        if !file_type.is_file() || !is_shard_file(&file_path) {
            continue;
        }

        fs::rename(&file_path, cache_dir.join(file_path.file_name().unwrap()))?;
    }

    Ok(())
}

/// Search from highest version, stop at version X where
/// a guard note of X is found in ref notes for X-1.
pub fn match_repo_mdb_version(
    repo_path: &Path,
    notesrefs: impl Fn(&ShardVersion) -> &'static str,
    highest_version: ShardVersion,
) -> errors::Result<ShardVersion> {
    let mut v = highest_version;

    while let Some(lower_v) = v.get_lower() {
        let guard_note = create_guard_note(&v)?;
        let lower_refnotes = notesrefs(&lower_v);
        if check_note_exists(repo_path, lower_refnotes, &guard_note)? {
            return Ok(v);
        }
        v = lower_v;
    }

    Ok(v)
}

/// Write a guard note for version X at ref notes for
/// all version below X.
pub fn write_mdb_version_guard_note(
    repo_path: &Path,
    notesrefs: impl Fn(&ShardVersion) -> &'static str,
    version: &ShardVersion,
) -> errors::Result<()> {
    let mut v = *version;

    let guard_note = create_guard_note(&v)?;

    while let Some(lower_v) = v.get_lower() {
        let lower_refnotes = notesrefs(&lower_v);
        add_note(repo_path, lower_refnotes, &guard_note)?;

        v = lower_v;
    }

    Ok(())
}

/// Create a guard note for a MDB version.
fn create_guard_note(version: &ShardVersion) -> errors::Result<Vec<u8>> {
    let mut buffer = Cursor::new(Vec::new());

    MerkleDBNotesHeader::new(version.get_value()).encode(&mut buffer)?;

    Ok(buffer.into_inner())
}

/// Put an empty MDBShardMetaCollection into the ref notes
pub async fn add_empty_note(config: &XetConfig, notesref: &str) -> errors::Result<()> {
    let note_with_empty_db =
        encode_shard_meta_collection_to_note(&MDBShardMetaCollection::default())?;
    add_note(config.repo_path()?, notesref, &note_with_empty_db)?;
    Ok(())
}

/// Queries a MerkleDB for a hash returning error if not found.
pub async fn query_merkledb(config: &XetConfig, hash: &str) -> errors::Result<()> {
    let shard_manager = ShardFileManager::new(&config.merkledb_v2_session).await?;
    shard_manager
        .register_shards_by_path(&[&config.merkledb_v2_cache])
        .await?;

    let hash = MerkleHash::from_hex(hash).map_err(|_| {
        GitXetRepoError::DataParsingError(format!("Cannot parse hash from {hash:?}"))
    })?;

    let file_info = shard_manager
        .get_file_reconstruction_info(&hash)
        .await?
        .ok_or(GitXetRepoError::HashNotFound)?;

    println!("{file_info:?}");
    Ok(())
}

/// Queries a MerkleDB for the total materialized and stored bytes,
/// print the result to stdout.
pub async fn cas_stat_git(config: &XetConfig) -> errors::Result<()> {
    let mut materialized_bytes = 0u64;
    let mut stored_bytes = 0u64;

    sync_mdb_shards_from_git(
        config,
        &config.merkledb_v2_cache,
        get_merkledb_notes_name(&ShardVersion::V2),
        false, // we don't want to fetch all shards to get repo size
    )
    .await?;

    let metas = MDBShardMetaCollection::open(&get_cache_meta_file(&config.merkledb_v2_cache)?)?;

    for meta in metas {
        materialized_bytes += meta.shard_footer.materialized_bytes;
        stored_bytes += meta.shard_footer.stored_bytes;
    }

    println!("{{");
    println!("\"total_cas_bytes\" : {stored_bytes},");
    println!("\"total_file_bytes\" : {materialized_bytes}");
    println!("}}");

    Ok(())
}

pub fn verify_mdb_shard_on_disk(shard_file: &Path) {
    MDBShardFile::load_from_file(&shard_file)
        .map_err(|e| {
            error!("Error loading file {:?}: {e:?}", &shard_file);
            e
        })
        .unwrap()
        .verify_shard_integrity();
}

pub async fn verify_mdb_shard_in_cas(
    config: &XetConfig,
    shard_hash: &MerkleHash,
    dest_dir: Option<PathBuf>,
) {
    let cas = create_cas_client(config)
        .await
        .map_err(|e| {
            error!("Error creating cas client: {e:?}");
            e
        })
        .unwrap();

    // Create a temp directory
    let stagedir = TempDir::new().unwrap();

    let cache_dir = dest_dir.unwrap_or(stagedir.path().to_path_buf());

    let shard_file = download_shard(config, &cas, shard_hash, &cache_dir)
        .await
        .map_err(|e| {
            error!("Error downloading shard: {e:?}");
            e
        })
        .unwrap();

    verify_mdb_shard_on_disk(&shard_file);
}

pub async fn verify_mdb_shard(config: &XetConfig, shard: &str) {
    if shard.starts_with("cas://") {
        let shard_hash = MerkleHash::from_hex(&shard["cas://".len()..])
            .map_err(|e| {
                error!("Error parsing hash : {e:?}");
                e
            })
            .unwrap();

        verify_mdb_shard_in_cas(config, &shard_hash, None).await;
    } else {
        verify_mdb_shard_on_disk(&PathBuf::from_str(shard).unwrap())
    }

    eprintln!("Shard file {shard:?} passed all checks.");
}

#[cfg(test)]
mod test {
    use rand::{rngs::SmallRng, RngCore, SeedableRng};
    use std::mem::size_of;

    use crate::merkledb_shard_plumb::decode_shard_meta_collection_from_note;

    use super::*;

    #[test]
    fn test_shard_notes_bidir() {
        let rand_collection = |seed: u64| -> MDBShardMetaCollection {
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut collection = MDBShardMetaCollection::default();
            for _ in 0..100 {
                let mut buffer = [0u8; size_of::<MDBShardMeta>()];
                rng.fill_bytes(&mut buffer);

                unsafe {
                    let meta: MDBShardMeta = std::mem::transmute(buffer);
                    collection.push(meta);
                }
            }
            collection
        };

        let collections = (0..3).map(rand_collection);

        let blobs = collections
            .clone()
            .map(|c| encode_shard_meta_collection_to_note(&c).unwrap_or_default());

        collections.zip(blobs).for_each(|(c, b)| {
            assert_eq!(
                decode_shard_meta_collection_from_note(&b).unwrap_or_default(),
                c
            );
        });
    }
}
