use crate::errors::Result;
use base64;
use bincode::Options;
use git2::{ObjectType, Repository};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};
use tracing::warn;

use super::ListObjectEntry;

#[derive(Default, Serialize, Deserialize, Clone, Debug, Copy)]
pub struct GitOid([u8; 20]);

impl GitOid {
    #[allow(dead_code)] // Used for testing
    fn oid(&self) -> git2::Oid {
        self.into()
    }
}

impl From<git2::Oid> for GitOid {
    fn from(value: git2::Oid) -> Self {
        Self(value.as_bytes().try_into().unwrap())
    }
}

impl From<&git2::Oid> for GitOid {
    fn from(value: &git2::Oid) -> Self {
        Self(value.as_bytes().try_into().unwrap())
    }
}

impl From<GitOid> for git2::Oid {
    fn from(value: GitOid) -> Self {
        Self::from_bytes(&value.0[..]).unwrap()
    }
}

impl From<&GitOid> for git2::Oid {
    fn from(value: &GitOid) -> Self {
        Self::from_bytes(&value.0[..]).unwrap()
    }
}

impl std::fmt::Display for GitOid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let g_oid: git2::Oid = self.into();
        <git2::Oid as std::fmt::Display>::fmt(&g_oid, f)
    }
}

/// One component of the path.
#[derive(Serialize, Deserialize, Clone)]
pub struct ContinuationSubToken {
    /// The name of the tree determining this listing.  
    pub name: String,

    /// The oid of the tree determining this listing.
    pub oid: GitOid,

    /// The index in the parent tree's lexicographically sorted list of entries.
    pub index: u64,

    /// A cache for the tree entries; this is never serialized.
    #[serde(skip)]
    pub tree_entries: Option<Arc<Vec<ListObjectEntry>>>,
}

/// In a partial listing, gives the location and path of the last returned object.
/// If current_location has no entries, then it represents a starting listing.
#[derive(Serialize, Deserialize, Clone)]
pub struct ContinuationToken {
    /// The commmit oid in the main repository that we're referencing.  
    pub commit_oid: GitOid, // Is just [u8 ; 20]

    /// Is this a recursive listing or not?  If so, descend into subtrees.  If not, only work with entries in this folder.
    pub recursive: bool,

    /// The global index -- just the number of entries returned so far.
    pub global_idx: u64,

    /// The index of the current tree from which to start the next intering.
    pub location: Vec<ContinuationSubToken>,
}

impl ContinuationToken {
    /// Create a string representation of the token string.
    pub fn as_token_string(&self) -> Result<String> {
        let version: u8 = 0; // Version number
        let mut bytes = vec![version]; // Start with version number

        // Serialize self with bincode and append to bytes
        let serialized = bincode::options().with_varint_encoding().serialize(self)?;
        bytes.extend(serialized);

        // Encode bytes to base64 string
        Ok(base64::encode(bytes))
    }

    /// Deserialize the token object from the string.
    pub fn from_token_string(token: &str) -> Result<Self> {
        // Decode base64 string to bytes
        let bytes = base64::decode(token).expect("Failed to decode base64 string");

        // Skip the version byte and deserialize the rest with bincode
        let token: Self = bincode::options()
            .with_varint_encoding()
            .deserialize(&bytes[1..])?;

        Ok(token)
    }
}

/// Return a list of objects in this tree (directory entry), sorted by name.
pub fn dir_list_entries_at_tree_oid(
    repo: &Arc<Repository>,
    tree_oid: GitOid, // The oid of the tree representing that directory.
) -> Result<Arc<Vec<ListObjectEntry>>> {
    // Returns a sorted list of dir entries, either blobs or directories, given by the tree oid
    let tree = repo.find_tree(tree_oid.into())?;
    let mut entries = Vec::new();

    for entry in tree.iter() {
        let object = entry.to_object(repo)?;

        let (size, is_tree) = match object.kind() {
            Some(ObjectType::Blob) => (object.as_blob().unwrap().size(), false),
            Some(ObjectType::Tree) => (0, true),
            _ => {
                warn!("Git object entry {object:?} in tree {tree:?} has type other than blob or tree.");
                continue; // Skip if it's neither a blob nor a tree
            }
        };

        entries.push(ListObjectEntry {
            path: entry.name().unwrap_or_default().to_string(),
            oid: entry.id().into(),
            size,
            is_tree,
        });
    }

    // Assuming ListObjectEntry implements Ord or PartialOrd based on name
    entries.sort_by(|a, b| a.path.cmp(&b.path));

    Ok(Arc::new(entries))
}

/// Returns at most num_entries.  If the list is complete, then None is returned for the ContinuationToken; otherwise
/// a contiuation token that allows this to be resumed where the previous list of entries left off.
///
/// The returned list of entries will be in lexicographic order.
pub fn next_list_entries(
    repo: &Arc<Repository>,
    mut token: ContinuationToken,
    max_num_entries: usize,
) -> Result<(Vec<ListObjectEntry>, Option<ContinuationToken>)> {
    // Fill out all the rest of the list entries here, updating the continuation token with the location of the next entry.
    let mut entries = Vec::with_capacity(max_num_entries);

    loop {
        // We are done.
        if token.location.is_empty() {
            return Ok((entries, None));
        }

        // If we have enough.
        if entries.len() >= max_num_entries {
            return Ok((entries, Some(token)));
        }

        // Now, See if we need to go back up the stack.
        {
            let cur = token.location.last_mut().unwrap();

            if cur.tree_entries.is_none() {
                cur.tree_entries = Some(dir_list_entries_at_tree_oid(repo, cur.oid)?);
            }

            if cur.index as usize
                >= cur
                    .tree_entries
                    .as_ref()
                    .map(|e| e.len())
                    .unwrap_or_default()
            {
                // Time to go back up one!
                token.location.pop();
                // Immediately increment that counter.
                if let Some(next_entry) = token.location.last_mut() {
                    next_entry.index += 1;
                }
                continue;
            }
        }

        let cur_entry = {
            let cur = token.location.last().unwrap();
            &cur.tree_entries.as_ref().unwrap()[cur.index as usize]
        };

        if cur_entry.is_tree && token.recursive {
            token.location.push(ContinuationSubToken {
                name: cur_entry.path.clone(),
                oid: cur_entry.oid.into(),
                index: 0,
                tree_entries: None,
            });
        } else {
            entries.push(cur_entry.clone());
            token.location.last_mut().unwrap().index += 1;
        }
    }
}

pub fn begin_list_entries(
    repo: &Arc<Repository>,
    commit_oid: GitOid,
    prefix: String,
    recursive: bool,
    max_num_entries: usize,
) -> Result<(Vec<ListObjectEntry>, Option<ContinuationToken>)> {
    let commit = repo.find_commit(commit_oid.into())?;
    let tree = commit.tree()?;

    // Here, you need to resolve 'prefix' to an actual path within the tree,
    // which may require traversing the tree. This is a simplification.
    // If 'prefix' leads directly to a blob, return its details.
    // If 'prefix' leads to a tree, proceed with listing its contents.

    // Simplified: Assuming 'prefix' is always a directory (tree) for this example
    let object = tree.get_path(Path::new(&prefix))?;
    let oid = object.id();

    match object.kind() {
        Some(ObjectType::Blob) => {
            let blob = repo.find_blob(oid)?;
            let entry = ListObjectEntry {
                path: prefix,
                oid: oid.into(),
                size: blob.size(),
                is_tree: false,
            };
            Ok((vec![entry], None))
        }
        Some(ObjectType::Tree) => {
            // If a directory, we start listing its entries with an initial continuation token
            let token = ContinuationToken {
                recursive,
                commit_oid,
                global_idx: 0,
                location: vec![ContinuationSubToken {
                    name: prefix,
                    oid: oid.into(),
                    index: 0,
                    tree_entries: None,
                }],
            };

            next_list_entries(repo, token, max_num_entries)
        }
        _ => Err(anyhow::anyhow!("Unsupported object type"))?,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git_integration::{create_commit, list_objects, ListObjectEntry};
    use crate::git_integration::{
        git_process_wrapping::run_git_captured, git_repo_plumbing::open_libgit2_repo,
    };
    use rand::prelude::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn get_listing_using_tokens(
        repo: &Arc<Repository>,
        commit_oid: GitOid,
        prefix: String,
        recursive: bool,
        serialize_deserialize: bool,
        break_points: &[usize],
    ) -> Vec<ListObjectEntry> {
        let mut break_points = Vec::from(break_points);

        break_points.push(10000); // Big enough for what

        let (mut entries, token) = begin_list_entries(
            repo,
            commit_oid,
            prefix,
            recursive,
            *break_points.first().unwrap(),
        )
        .unwrap();

        if let Some(mut token) = token {
            for n in &break_points[1..] {
                let (new_items, new_token) = next_list_entries(repo, token, *n).unwrap();
                entries.extend(new_items);

                if let Some(t) = new_token {
                    // Serialize and deserialize
                    if serialize_deserialize {
                        let s = t.as_token_string().unwrap();
                        token = ContinuationToken::from_token_string(&s).unwrap();
                    } else {
                        token = t;
                    }
                } else {
                    break;
                }
            }
        }

        entries
    }

    fn get_listing_using_list_dir(
        repo: &Arc<Repository>,
        commit_oid: GitOid,
        prefix: String,
        recursive: bool,
    ) -> Vec<ListObjectEntry> {
        let oid = commit_oid.oid().to_string();

        let mut entries = list_objects(repo, &prefix, Some(&oid), recursive).unwrap();

        entries.sort_by(|e1, e2| e1.path.cmp(&e2.path));

        entries
    }

    fn verify_git_repo_at_commit(
        repo: &Arc<Repository>,
        commit_oid: GitOid,
        prefix: &str,
        recursive: bool,
        known_file_sizes: &[(String, usize)],
    ) {
        let mut known_file_sizes: HashMap<_, _> = known_file_sizes.iter().cloned().collect();

        // Get the base listing:
        let correct_results =
            get_listing_using_list_dir(repo, commit_oid, prefix.to_owned(), recursive);

        for break_points in &[
            vec![],
            vec![1, 2, 4, 8, 1, 2, 4, 8, 10, 12],
            vec![8; 100],
            vec![1; 1000],
        ] {
            let test_results = get_listing_using_tokens(
                repo,
                commit_oid,
                prefix.to_owned(),
                recursive,
                true,
                break_points,
            );

            // Make sure they are equal
            assert_eq!(correct_results, test_results);

            // Check against the base sizes
            for loe in test_results {
                if let Some(s) = known_file_sizes.get(&loe.path).clone() {
                    assert_eq!(*s, loe.size);
                    known_file_sizes.remove(&loe.path);
                }
            }

            // And everything is there.
            if recursive {
                assert_eq!(known_file_sizes.len(), 0);
            }
        }
    }

    fn make_random_commit(
        repo: &Arc<Repository>,
        branch: &str,
        files_and_sizes: &[(String, usize)],
    ) -> GitOid {
        let mut rng = StdRng::seed_from_u64(0);

        let mut file_map = Vec::new();

        for (path, size) in files_and_sizes {
            let mut file = vec![0u8; *size];
            // Fill file with random bytes
            rng.fill_bytes(&mut file);
            file_map.push((path, file));
        }

        let file_list: Vec<(&str, &[u8])> =
            file_map.iter().map(|(s, d)| (s.as_str(), &d[..])).collect();

        create_commit(
            repo,
            Some(branch),
            "Test commit",
            &file_list[..],
            None,
            None,
        )
        .unwrap()
        .into()
    }

    fn make_random_structures(dirs_and_quantities: &[(&str, usize)]) -> Vec<(String, usize)> {
        let mut rng = StdRng::seed_from_u64(0);

        let mut ret = Vec::with_capacity(dirs_and_quantities.iter().map(|(_, s)| s).sum());

        for (p, s) in dirs_and_quantities {
            for i in 0..*s {
                ret.push((format!("{p}/f_{i}.dat"), rng.gen_range(1..256)));
            }
        }

        ret
    }

    #[test]
    fn test_simple_repo() {
        // Create a temporary directory
        let tmp_repo = TempDir::new().unwrap();
        let tmp_repo_path = tmp_repo.path().to_path_buf();

        let _ = run_git_captured(Some(&tmp_repo_path), "init", &["--bare"], true, None);

        let repo = open_libgit2_repo(Some(&tmp_repo_path)).unwrap();

        // 10 files in the root directory.
        let files_1 = make_random_structures(&[(".", 10)]);
        let commit_1 = make_random_commit(&repo, "main", &files_1);
        verify_git_repo_at_commit(&repo, commit_1, "", false, &files_1);
        verify_git_repo_at_commit(&repo, commit_1, "", true, &files_1);

        // Add 5 files in a sub directory.
        let files_2 = make_random_structures(&[("data/", 5)]);
        let commit_2 = make_random_commit(&repo, "main", &files_2);
        verify_git_repo_at_commit(&repo, commit_2, "", false, &files_2);
        verify_git_repo_at_commit(&repo, commit_2, "", true, &files_2);
        verify_git_repo_at_commit(&repo, commit_2, "data/", false, &files_2);
        verify_git_repo_at_commit(&repo, commit_2, "data/", true, &files_2);

        // Add in a deeply nested directory to a different commit.
        let files_3 = make_random_structures(&[
            ("other_data/", 2),
            ("other_data/a/", 2),
            ("other_data/a/b/", 2),
            ("other_data/a/c/", 2),
            ("other_data/a/c/d/e/f/g/h/i/j/k", 10),
        ]);
        let commit_3 = make_random_commit(&repo, "branch1", &files_3);
        verify_git_repo_at_commit(&repo, commit_3, "", false, &files_3);
        verify_git_repo_at_commit(&repo, commit_3, "", true, &files_3);
        verify_git_repo_at_commit(&repo, commit_3, "other_data", false, &files_3);
        verify_git_repo_at_commit(&repo, commit_3, "other_data", true, &files_3);
        verify_git_repo_at_commit(&repo, commit_3, "other_data/a/c/", false, &files_3);
        verify_git_repo_at_commit(&repo, commit_3, "other_data/a/c/", true, &files_3);

        // Add in a nested directory back to main.
        let files_4 = make_random_structures(&[("other_data/a/c/d/e/f/", 25)]);
        let commit_4 = make_random_commit(&repo, "main", &files_4);
        verify_git_repo_at_commit(&repo, commit_4, "", false, &files_4);
        verify_git_repo_at_commit(&repo, commit_4, "", true, &files_4);
        verify_git_repo_at_commit(&repo, commit_4, "other_data", false, &files_4);
        verify_git_repo_at_commit(&repo, commit_4, "other_data", true, &files_4);
        verify_git_repo_at_commit(&repo, commit_4, "other_data/a/c/", false, &files_4);
        verify_git_repo_at_commit(&repo, commit_4, "other_data/a/c/", true, &files_4);

        // Now, just go back and verify everything a second time.

        verify_git_repo_at_commit(&repo, commit_1, "", false, &files_1);
        verify_git_repo_at_commit(&repo, commit_1, "", true, &files_1);
        verify_git_repo_at_commit(&repo, commit_2, "", false, &files_2);
        verify_git_repo_at_commit(&repo, commit_2, "", true, &files_2);
        verify_git_repo_at_commit(&repo, commit_2, "data/", false, &files_2);
        verify_git_repo_at_commit(&repo, commit_2, "data/", true, &files_2);
        verify_git_repo_at_commit(&repo, commit_3, "", false, &files_3);
        verify_git_repo_at_commit(&repo, commit_3, "", true, &files_3);
        verify_git_repo_at_commit(&repo, commit_3, "other_data", false, &files_3);
        verify_git_repo_at_commit(&repo, commit_3, "other_data", true, &files_3);
        verify_git_repo_at_commit(&repo, commit_3, "other_data/a/c/", false, &files_3);
        verify_git_repo_at_commit(&repo, commit_3, "other_data/a/c/", true, &files_3);
        verify_git_repo_at_commit(&repo, commit_4, "", false, &files_4);
        verify_git_repo_at_commit(&repo, commit_4, "", true, &files_4);
        verify_git_repo_at_commit(&repo, commit_4, "other_data", false, &files_4);
        verify_git_repo_at_commit(&repo, commit_4, "other_data", true, &files_4);
        verify_git_repo_at_commit(&repo, commit_4, "other_data/a/c/", false, &files_4);
        verify_git_repo_at_commit(&repo, commit_4, "other_data/a/c/", true, &files_4);
    }
}
