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
    use crate::git_integration::{list_objects, ListObjectEntry};

    use super::*;

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

        entries.sort_by_key(|e| &e.path);

        entries
    }
}
