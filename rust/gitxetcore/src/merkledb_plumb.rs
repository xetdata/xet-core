use crate::config::XetConfig;
use crate::constants::{POINTER_FILE_LIMIT, SMALL_FILE_THRESHOLD};
use crate::errors;
use crate::git_integration::GitRepo;
use crate::standalone_pointer::*;
use anyhow;
use anyhow::Context;
use bincode::Options;
use cas::output_bytes;
use git2::Repository;
use merkledb::prelude_v2::*;
use merkledb::*;
use merklehash::*;
use pointer_file::PointerFile;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use tracing::{debug, error};

pub const MERKLEDB_NOTES_ENCODING_VERSION: u64 = 1;
pub const MERKLEDB_NOTES_HEADER_SIZE: usize = 16;

/// MerkleDB Notes Encoding
///
/// To allow us to change the MerkleDB Notes encoding format while being able
/// to parse the old layout we use a little trick that the MerkleMemDB is never
/// empty. It always has at least 1 node in it.  As such, a truly empty
/// MerkleMemDB with 0 nodes will never be serialized. With the
/// bincode serialization layout, this means that if the first
/// uint64_t (8  bytes) = 0, we can use that to denote a format change.
///
/// ```ignore
/// For a merkledb notes entry:
/// [word1 : 8 bytes][word2 : 8 bytes]...
///
/// if word1 == 0:
///    word2 is a version indicator
/// if word1 != 0:
///    the entire file is a version 0 MerkleMemDB (what merkleMemDB is today)
///
/// Version 1 format:
/// When word2 == 1 (version = 1):
///    the remaining bytes are a Standalone Pointer
/// ```
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MerkleDBNotesHeader {
    header: u64,
    version: u64,
}
impl MerkleDBNotesHeader {
    pub fn new(version: u64) -> Self {
        MerkleDBNotesHeader { header: 0, version }
    }
    pub fn decode(bytes: &[u8]) -> errors::Result<Self> {
        if bytes.len() < MERKLEDB_NOTES_HEADER_SIZE {
            return Err(errors::GitXetRepoError::DataParsingError(
                "Insufficient bytes to decode a MerkleDB".into(),
            ));
        }
        let options = bincode::DefaultOptions::new().with_fixint_encoding();
        // since we checked the length above. this should always decode fine.
        Ok(options.deserialize_from(bytes).unwrap())
    }
    pub fn encode(&self, write: impl io::Write) -> errors::Result<()> {
        let options = bincode::DefaultOptions::new().with_fixint_encoding();
        options.serialize_into(write, self).map_err(|_| {
            errors::GitXetRepoError::Other("Unable to serialize MerkleDB Notes header".into())
        })?;
        Ok(())
    }
    pub fn get_version(&self) -> u64 {
        // if the header is non-zero, this is a version 0 file.
        if self.header != 0 {
            0
        } else {
            self.version
        }
    }
}

/// Decode a MerkleDB from a notes entry
async fn decode_db_from_note(config: &XetConfig, bytes: &[u8]) -> errors::Result<MerkleMemDB> {
    let header = MerkleDBNotesHeader::decode(bytes)?;
    debug!("Parsed a MDB header {:?}", header);
    let version = header.get_version();
    if version == 0 {
        Ok(MerkleMemDB::open_reader(bytes)?)
    } else if version == 1 {
        let remaining_bytes = &bytes[MERKLEDB_NOTES_HEADER_SIZE..];
        let mut file = Cursor::new(Vec::new());
        standalone_pointer_to_file(config, remaining_bytes, &mut file)
            .await
            .map_err(|e| {
                error!("Unable to decode a MerkleDB {:?}", e);
                e
            })?;
        Ok(MerkleMemDB::open_reader(&file.into_inner()[..])?)
    } else {
        // future version
        panic!("Encountering version {version} of MerkleDB format. Please upgrade git-xet");
    }
}

/// Encodes a MerkleDB to a notes entry
pub async fn encode_db_to_note(config: &XetConfig, mut db: MerkleMemDB) -> errors::Result<Vec<u8>> {
    // serialize the diff into memory
    let mut file = Cursor::new(Vec::new());
    db.write_into(&mut file);
    if file.position() < SMALL_FILE_THRESHOLD as u64 {
        Ok(file.into_inner())
    } else {
        file.set_position(0);
        // convert it to a standalone pointer file
        let mut pointerfile = Cursor::new(Vec::new());
        MerkleDBNotesHeader::new(MERKLEDB_NOTES_ENCODING_VERSION).encode(&mut pointerfile)?;
        file_to_standalone_pointer(config, file, &mut pointerfile).await?;
        Ok(pointerfile.into_inner())
    }
}

/*
Clap CLI implementations
*/

/// Finds the current merkledb location in a given repository path, or if not
/// provided, searches in the current directory
pub fn find_git_db(searchpath: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    let searchpath = match searchpath {
        Some(p) => p,
        None => std::env::current_dir().with_context(|| "Unable to find current directory")?,
    };
    let repo = Repository::discover(searchpath)?;
    Ok(repo.path().join("xet").join("merkledb.db"))
}

/// Obtains the repository path either from config or current directory
pub fn get_repo_path_from_config(config: &XetConfig) -> anyhow::Result<PathBuf> {
    match config.repo_path() {
        Ok(path) => Ok(path.clone()),
        Err(_) => std::env::current_dir().with_context(|| "Unable to find current directory"),
    }
}
/// Merges a collection of merkledbs into a single output merkledb
pub fn merge_merkledb(output: &Path, inputs: &Vec<PathBuf>) -> anyhow::Result<()> {
    let mut outdb =
        MerkleMemDB::open(output).with_context(|| format!("Unable to open file {output:?}"))?;
    for i in inputs {
        let idb = MerkleMemDB::open(i).with_context(|| format!("Unable to open file {i:?}"))?;
        outdb.union_with(&idb);
    }
    outdb.union_finalize()?;
    outdb.flush()?;
    Ok(())
}

/// Computes the diff of a collection of merkledbs
pub fn diff_merkledb(older: &Path, newer: &Path, result: &Path) -> anyhow::Result<()> {
    let mut result =
        MerkleMemDB::open(result).with_context(|| format!("Unable to open file {result:?}"))?;
    let older =
        MerkleMemDB::open(older).with_context(|| format!("Unable to open file {older:?}"))?;
    let newer =
        MerkleMemDB::open(newer).with_context(|| format!("Unable to open file {newer:?}"))?;
    result.difference(&newer, &older);
    result.flush()?;
    Ok(())
}

/// Prints a merkledb to stdout
pub fn print_merkledb(input: &Path) -> anyhow::Result<()> {
    let input =
        MerkleMemDB::open(input).with_context(|| format!("Unable to open file {input:?}"))?;
    println!("{input:?}");
    Ok(())
}

/// Queries a MerkleDB for a hash returning everything we know about it
pub fn query_merkledb(input: &Path, hash: &String) -> anyhow::Result<()> {
    let input =
        MerkleMemDB::open(input).with_context(|| format!("Unable to open file {input:?}"))?;
    let hash = MerkleHash::from_hex(hash).with_context(|| format!("{hash} is an invalid hash"))?;
    let node = input
        .find_node(&hash)
        .with_context(|| format!("Hash {hash} not found"))?;
    println!("{}", input.print_node(&node));
    println!("{}", input.print_node_details(&node));
    Ok(())
}

/// Check if the ref notes contains an empty MerkleMemDB
pub async fn check_merklememdb_is_empty(
    config: &XetConfig,
    localdb: &Path,
    notesref: &str,
) -> anyhow::Result<bool> {
    // Check if there's data in the local merkledb file
    let localdb =
        MerkleMemDB::open(localdb).with_context(|| format!("Unable to open file {localdb:?}"))?;

    if !localdb.is_empty() {
        return Ok(false);
    }

    drop(localdb);
    let repo = GitRepo::open(config.repo_path().ok().map(|p| p.as_path()))
        .with_context(|| format!("Unable to access git notes at {notesref:?}"))?;

    // Check if there's data in the ref notes
    let iter = repo_notes.notes_name_iterator()?;

    // There may exist an empty MerkleMemDB in the note
    if iter.count() > 1 {
        return Ok(false);
    }

    // Further check if the db in the note is empty
    let mut iter = repo_notes.xet_notes_content_iterator(notesref)?;
    if let Some((_, blob)) = iter.next() {
        let memdb = decode_db_from_note(config, &blob).await?;
        Ok(memdb.is_empty())
    } else {
        Ok(true)
    }
}

/// Put an empty MerkleMemDB into the ref notes
pub async fn add_empty_note(config: &XetConfig, notesref: &str) -> anyhow::Result<()> {
    let note_with_empty_db = encode_db_to_note(config, MerkleMemDB::default()).await?;
    GitRepo::open(Some(config.repo_path()?))?.add_xet_note(notesref, &note_with_empty_db)?;
    Ok(())
}

/// Aggregates all the MerkleDBs stored in git notes into a single DB
pub async fn merge_db_from_git(
    config: &XetConfig,
    db: &mut MerkleMemDB,
    notesref: &str,
) -> anyhow::Result<()> {
    let repo = GitRepo::open(config.repo_path().ok().map(|p| p.as_path()))
        .with_context(|| format!("Unable to access git notes at {notesref:?}"))?;
    for oid_r in repo
        .xet_notes_name_iterator(notesref)
        .with_context(|| format!("Unable to iterate over git notes at {notesref:?}"))?
    {
        // we use the hash of the note name as an indicator if this note
        // has already been merged into the db
        let oidhash = compute_internal_node_hash(oid.as_bytes());
        if db.find_node(&oidhash).is_some() {
            continue;
        }
        if let Ok(blob) = repo_notes.notes_name_to_content(&oid) {
            let memdb = decode_db_from_note(config, &blob)
                .await
                .with_context(|| format!("Unable to parse notes entry at {oid}"))?;
            db.union_with(&memdb);

            // after unioning, we add a "fake chunk"
            // to denote that the note has already been added to this memdb
            // and so we can avoid adding it again.
            db.add_chunk(&Chunk {
                hash: oidhash,
                length: oid.len(),
            });
        }
    }
    db.union_finalize()?;
    db.flush()?;
    Ok(())
}

/// Aggregates all the MerkleDBs stored in git notes into a single DB
pub async fn merge_merkledb_from_git(
    config: &XetConfig,
    output: &Path,
    notesref: &str,
) -> anyhow::Result<()> {
    let mut outdb =
        MerkleMemDB::open(output).with_context(|| format!("Unable to open file {output:?}"))?;
    merge_db_from_git(config, &mut outdb, notesref).await
}
pub async fn update_merkledb_to_git(
    config: &XetConfig,
    input: &Path,
    notesref: &str,
) -> anyhow::Result<()> {
    // open the input db
    let inputdb =
        MerkleMemDB::open(input).with_context(|| format!("Unable to open file {input:?}"))?;

    // figure out the db contents of git
    let mut gitdb = MerkleMemDB::default();
    merge_db_from_git(config, &mut gitdb, notesref).await?;

    // calculate the diff
    let mut diffdb = MerkleMemDB::default();
    diffdb.difference(&inputdb, &gitdb);

    // save some memory
    drop(inputdb);
    drop(gitdb);
    let vec = encode_db_to_note(config, diffdb).await?;

    let repo = GitRepo::open(config.repo_path().ok().map(|p| p.as_path())).map_err(|e| {
        error!("update_merkledb_to_git: unable to access git notes at {notesref:?}: {e:?}");
        e
    })?;

    let repo_notes = repo.notes_wrapper(notesref);

    repo_notes.add_note(vec).map_err(|e| {
        error!("update_merkledb_to_git: Error inserting new note in set_repo_salt ({notesref:?}: {e:?}");
        e
    })?;

    Ok(())
}
pub fn list_git(config: &XetConfig, notesref: &str) -> anyhow::Result<()> {
    let repo = GitRepo::open(config.repo_path().ok().map(|p| p.as_path())).map_err(|e| {
        error!("update_merkledb_to_git: unable to access git notes at {notesref:?}: {e:?}");
        e
    })?;

    let repo_notes = repo.notes_wrapper(notesref);
    println!("id, nodes ,db-bytes");
    for (oid, blob) in repo_notes.notes_content_iterator()? {
        let file = Cursor::new(&blob);
        if let Ok(memdb) = MerkleMemDB::open_reader(file) {
            println!("{}, {}, {}", oid, memdb.get_sequence_number(), blob.len());
        } else {
            eprintln!("Unable to decode blob {oid}");
        }
    }
    Ok(())
}

const MINHASH_SIG_SIZE: usize = 32;
#[derive(Clone, Debug)]

/// A rudimentrary MinHash mechanism for estimating simiarity scores
/// based on building 32 16-bit hashes.
pub struct MinHashSig([u16; MINHASH_SIG_SIZE]);
impl MinHashSig {
    /// Expands a 256-bit hash into 32 16-bit hashes
    /// We derive the first 16 hashes directly from the 256-bit hash
    /// and the next 16 hashes by hashing the hash
    fn from_hash(hash: &MerkleHash) -> MinHashSig {
        let mut ret = [0; MINHASH_SIG_SIZE];
        let first: [u16; 16] = unsafe { std::mem::transmute(*hash) };
        ret[..16].clone_from_slice(&first);
        // we make a second hash by hashing the hash
        let newhash = compute_data_hash(hash.hex().as_bytes());
        let second: [u16; 16] = unsafe { std::mem::transmute(newhash) };
        ret[16..].clone_from_slice(&second);
        MinHashSig(ret)
    }
    /// Returns a hash comprising of the maximum
    fn new_max() -> MinHashSig {
        MinHashSig([u16::MAX; MINHASH_SIG_SIZE])
    }
    /// returns true if this array is uninitialized (i.e. all new_max())
    fn is_uninit(&self) -> bool {
        for i in self.0 {
            if i != u16::MAX {
                return false;
            }
        }
        true
    }
    /// Calculates a similarity score (Jaccard similarity) between 0 and 1
    fn similarity(&self, other: &MinHashSig) -> f32 {
        let mut equal: usize = 0;
        // if everything is MAX (new_max()) it is an empty file
        // and we assume that nothing is similar to the empty file
        if self.is_uninit() || other.is_uninit() {
            return 0.0;
        }
        for i in 0..MINHASH_SIG_SIZE {
            equal += (self.0[i] == other.0[i]) as usize;
        }
        equal as f32 / MINHASH_SIG_SIZE as f32
    }
    fn inplace_min(&mut self, b: &MinHashSig) {
        for i in 0..MINHASH_SIG_SIZE {
            self.0[i] = self.0[i].min(b.0[i]);
        }
    }
}

/// For a given blob, returns all the CAS entry the blob uses and the
/// number of bytes covered by each CAS entry
pub fn find_cas_nodes_for_blob(
    mdb: &MerkleMemDB,
    blob: &git2::Blob,
) -> anyhow::Result<(HashMap<MerkleHash, usize>, MinHashSig)> {
    let mut casret = HashMap::new();
    let mut sig = MinHashSig::new_max();
    if blob.size() <= POINTER_FILE_LIMIT {
        if let Ok(utf) = std::str::from_utf8(blob.content()) {
            let pointer = PointerFile::init_from_string(utf, "");
            if pointer.is_valid() {
                if let Some(node) = mdb.find_node(&pointer.hash()?) {
                    // compute the minhash signature which is
                    // an inplace min over all the leaves
                    for n in mdb.find_all_leaves(&node).unwrap() {
                        sig.inplace_min(&MinHashSig::from_hash(n.hash()));
                    }
                    let block_v = mdb.reconstruct_from_cas(&[node])?;
                    for i in block_v[0].1.iter() {
                        let counter = casret.entry(i.hash).or_insert(0);
                        *counter += i.end - i.start;
                    }
                }
            }
        }
    }
    Ok((casret, sig))
}

/// Returns 3 things
/// 1. a set of every CAS node used in this tree.
/// 2: for each OID, in the tree returns all the CAS entry the blob uses and the
/// number of bytes covered each CAS entry.
/// 3: A minhash signature for each OID
/// 4: file name to OID mapping (note that identical files may have the same OID)
#[allow(clippy::type_complexity)]
fn list_all_cas_nodes_and_pointers_for_tree(
    mdb: &MerkleMemDB,
    repo: &Repository,
    tree: &git2::Tree,
) -> (
    HashSet<MerkleHash>,
    HashMap<git2::Oid, HashMap<MerkleHash, usize>>,
    HashMap<String, MinHashSig>,
    HashMap<String, git2::Oid>,
) {
    let mut cas = HashSet::new();
    let mut oids = HashMap::new();
    let mut sigs = HashMap::new();
    let mut fnames = HashMap::new();
    tree.walk(git2::TreeWalkMode::PreOrder, |_, entry| {
        if let Some(git2::ObjectType::Blob) = entry.kind() {
            let blob = entry.to_object(repo).unwrap().peel_to_blob().unwrap();
            if let Ok((casuse, sig)) = find_cas_nodes_for_blob(mdb, &blob) {
                for h in casuse.keys() {
                    cas.insert(*h);
                }
                oids.insert(blob.id(), casuse);
                sigs.insert(entry.name().unwrap().to_string(), sig);
                fnames.insert(entry.name().unwrap().to_string(), blob.id());
            }
        }

        git2::TreeWalkResult::Ok
    })
    .unwrap();
    (cas, oids, sigs, fnames)
}
fn find_most_similar_file(src: &String, all: &HashMap<String, MinHashSig>) -> (String, f32) {
    let mut best_file = src.clone();
    let cursig: MinHashSig = match all.get(&best_file) {
        Some(x) => (*x).clone(),
        None => {
            return (best_file, 1.0);
        }
    };
    let mut best_similarity_score: f32 = 0.0;
    for (k, v) in all {
        if k == src {
            continue;
        }
        let newsim = cursig.similarity(v);
        if newsim > best_similarity_score {
            best_similarity_score = newsim;
            best_file = k.clone();
        }
    }
    (best_file, best_similarity_score)
}

pub fn stat_git(
    mdb: &Path,
    reference: &str,
    print_change_stats: bool,
    print_similarity: bool,
) -> anyhow::Result<()> {
    let repo = Repository::discover(
        std::env::current_dir().with_context(|| "Unable to find current directory")?,
    )
    .map_err(|_| anyhow::anyhow!("Not in a git repo"))?;
    let oid = repo
        .revparse_single(reference)
        .map_err(|_| anyhow::anyhow!("Unable to resolve reference {}", reference))?
        .id();
    let commit = repo
        .find_commit(oid)
        .map_err(|_| anyhow::anyhow!("reference is not a commit"))?;
    if commit.parents().len() > 1 {
        return Err(anyhow::anyhow!("Reference points to a merge commit. Merge commit statistics are not supported at the moment"));
    }
    let mdb = MerkleMemDB::open(mdb).with_context(|| format!("Unable to open {mdb:?}"))?;

    // Find the current tree
    let curtree = commit.tree()?;
    // find the list of cas entries used by this tree (curtree_nodes)
    // and for each oid in the tree, the CAS entries used (curtree_oids)
    let (curtree_nodes, curtree_oids, curtree_sigs, curtree_names) =
        list_all_cas_nodes_and_pointers_for_tree(&mdb, &repo, &curtree);

    // Find the previous tree if any
    let prevtree = if commit.parents().len() == 0 {
        None
    } else {
        Some(commit.parent(0)?.tree()?)
    };

    // find the list of cas entries used by the previous tree (prevtree_nodes)
    // and for each oid in the tree, the CAS entries used (prevtree_oids)
    // prevtree_oids is currently unused. We could probably do more
    // stats with it.
    let (prevtree_nodes, _, _, _) = if let Some(tree) = &prevtree {
        list_all_cas_nodes_and_pointers_for_tree(&mdb, &repo, tree)
    } else {
        (
            HashSet::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
        )
    };

    // find all the new cas nodes
    let cas_diff = &curtree_nodes - &prevtree_nodes;
    let new_cas_entries_count = cas_diff.len();
    // calculate the total cas size
    let total_xorb_bytes: usize = cas_diff
        .iter()
        .filter_map(|x| mdb.find_node(x))
        .map(|x| x.len())
        .sum();

    // Ask git for the tree diff
    let treediff = repo
        .diff_tree_to_tree(
            prevtree.as_ref(),
            Some(&curtree),
            Some(
                git2::DiffOptions::new()
                    .ignore_filemode(true)
                    .ignore_submodules(true)
                    .force_binary(true), // we just want to know what files changed
            ),
        )
        .map_err(|_| anyhow::anyhow!("Unable to compute diff"))?;

    // for each changed file
    // count the number of bytes used by the new cas entries
    let mut total_modified_file_size: usize = 0;
    let mut file_bytes_in_new_xorbs: usize = 0;
    if print_change_stats {
        println!("Filename, File size, Bytes Required For Changes");
    }
    for delta in treediff.deltas() {
        let delta_new_file = delta.new_file();
        let curfile = delta_new_file.id();
        let mut new_file_size = 0;
        let mut new_xorb_coverage = 0;
        if let Some(casuse) = curtree_oids.get(&curfile) {
            for i in casuse.iter() {
                if cas_diff.contains(i.0) {
                    new_xorb_coverage += i.1;
                }
                new_file_size += i.1;
            }
        }

        if new_file_size > 0 {
            total_modified_file_size += new_file_size;
            file_bytes_in_new_xorbs += new_xorb_coverage;
            if print_change_stats {
                println!(
                    "{:?}, {}, {}",
                    delta_new_file.path().unwrap(),
                    new_file_size,
                    new_xorb_coverage
                );
            }
        }
    }

    if print_similarity {
        println!("Filename, Most Similar File, Approximate Similarity Score");
        for (curname, _) in curtree_names.iter() {
            let (similar_file, similar_score) = find_most_similar_file(curname, &curtree_sigs);
            if similar_score >= 0.5 {
                println!("{curname}, {similar_file}, {similar_score}");
            }
        }
    }

    if !print_change_stats && !print_similarity {
        println!();
        println!("Summary");
        println!("-------");
        println!("New blocks created: {new_cas_entries_count} blocks");
        println!(
            "Total bytes in new blocks: {}",
            output_bytes(total_xorb_bytes)
        );
        println!(
            "Total size of all modified files: {}",
            output_bytes(total_modified_file_size)
        );
        println!(
            "File bytes reused from prior blocks: {}",
            output_bytes(total_modified_file_size - file_bytes_in_new_xorbs)
        );
        println!(
            "File bytes in new blocks: {}",
            output_bytes(file_bytes_in_new_xorbs)
        );
        println!(
            "Bytes saved to intra-commit deduplication: {}",
            output_bytes(file_bytes_in_new_xorbs - total_xorb_bytes)
        );
    }
    Ok(())
}

pub fn cas_stat_git(mdb: &Path) -> anyhow::Result<()> {
    let mdb = MerkleMemDB::open(mdb).with_context(|| format!("Unable to open {mdb:?}"))?;
    let mut total_cas_size = 0_usize;
    let mut total_cas_count = 0_usize;
    let mut total_file_size = 0_usize;
    let mut total_file_count = 0_usize;
    for node in mdb.node_iterator() {
        if let Some(attr) = mdb.node_attributes(node.id()) {
            if attr.is_cas() {
                total_cas_size += node.len();
                total_cas_count += 1;
            }
            if attr.is_file() {
                total_file_size += node.len();
                total_file_count += 1;
            }
        }
    }
    println!("{{");
    println!("\"total_cas_nodes\" : {total_cas_count},");
    println!("\"total_cas_bytes\" : {total_cas_size},");
    println!("\"total_file_nodes\" : {total_file_count},");
    println!("\"total_file_bytes\" : {total_file_size}");
    println!("}}");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use merklehash::compute_data_hash;
    use tempfile::TempDir;

    #[tokio::test]

    async fn test_small_mdb_notes_bidir() {
        // test that small merkledbs go forward and backward
        let stagedir = TempDir::new().unwrap();
        let config = XetConfig {
            staging_path: Some(stagedir.path().into()),
            ..Default::default()
        };
        let mut mdb = MerkleMemDB::default();
        // make a little mdb
        let h1 = compute_data_hash("hello world".as_bytes());
        let chunk = Chunk {
            hash: h1,
            length: 11,
        };
        let _ = mdb.add_chunk(&chunk);

        //encode
        let vec = encode_db_to_note(&config, mdb).await.unwrap();

        // verify this is indeed a version 0 header
        let header = MerkleDBNotesHeader::decode(&vec).unwrap();
        assert_eq!(header.get_version(), 0);

        // decode
        let mdb2 = decode_db_from_note(&config, &vec).await.unwrap();
        // compare
        assert!(mdb2.find_node(&chunk.hash).is_some());
    }

    fn generate_random_chunks(count: usize) -> Vec<Chunk> {
        use rand::{RngCore, SeedableRng};

        let mut rng = rand::rngs::StdRng::seed_from_u64(1234);
        let mut ret = Vec::new();
        // make a whole bunch of random chunks
        for _ in 0..count {
            let mut bytes: [u8; 32] = [0; 32];
            rng.fill_bytes(&mut bytes);
            ret.push(Chunk {
                hash: MerkleHash::try_from(bytes.as_slice()).unwrap(),
                length: 1234,
            });
        }
        ret
    }

    #[tokio::test]
    async fn test_large_mdb_notes_bidir() {
        // test that large merkledbs go forward and backward
        let stagedir = TempDir::new().unwrap();
        let config = XetConfig {
            staging_path: Some(stagedir.path().into()),
            ..Default::default()
        };
        let mut mdb = MerkleMemDB::default();
        // make a bigg-ish MDB of random stuff
        for chunk in generate_random_chunks(10000).into_iter() {
            let _ = mdb.add_chunk(&chunk);
        }
        let mdb1 = mdb.clone();

        //encode
        let vec = encode_db_to_note(&config, mdb).await.unwrap();

        // verify this should be long enough to make a version 1 header
        let header = MerkleDBNotesHeader::decode(&vec).unwrap();
        assert_eq!(header.get_version(), 1);

        // decode
        let mdb2 = decode_db_from_note(&config, &vec).await.unwrap();

        // compare
        let mut diffdb = MerkleMemDB::default();
        diffdb.difference(&mdb2, &mdb1);
        // the mdb2 and mdb1 should be identical.
        // (diff node count is 1. Remember a MerkleDB will never be
        // fully empty. There is always the node of hash=0);
        let nnodes = diffdb.node_iterator().count();
        assert_eq!(nnodes, 1);
    }

    #[tokio::test]
    async fn test_backcompact_v0_decode() {
        // test that large merkledbs encoded in the classical way
        // decode with decode_db_from_nodes
        let stagedir = TempDir::new().unwrap();
        let config = XetConfig {
            staging_path: Some(stagedir.path().into()),
            ..Default::default()
        };
        let mut mdb = MerkleMemDB::default();
        // make a bigg-ish MDB of random stuff
        for chunk in generate_random_chunks(10000).into_iter() {
            let _ = mdb.add_chunk(&chunk);
        }
        let mdb1 = mdb.clone();

        // encode
        let mut file = Cursor::new(Vec::new());
        mdb.write_into(&mut file);
        let vec = file.into_inner();

        // verify that this parses as a version 0 header
        let header = MerkleDBNotesHeader::decode(&vec).unwrap();
        assert_eq!(header.get_version(), 0);

        // decode
        let mdb2 = decode_db_from_note(&config, &vec).await.unwrap();

        // compare
        let mut diffdb = MerkleMemDB::default();
        diffdb.difference(&mdb2, &mdb1);
        // the mdb2 and mdb1 should be identical.
        // (diff node count is 1. Remember a MerkleDB will never be
        // fully empty. There is always the node of hash=0);
        let nnodes = diffdb.node_iterator().count();
        assert_eq!(nnodes, 1);
    }
}
