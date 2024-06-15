use git2::{Oid, Repository};
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashMap;

// Detects OIDs stored in commit messages like merges.  Minimum 6 characters.
lazy_static! {
    static ref OID_REGEX: Regex = Regex::new(r"\b[0-9a-fA-F]{6,40}\b").unwrap();
}

/// Extracts all the valid source oids present in a message.
pub fn extract_str_oids(src: &Repository, message: &str) -> Vec<Oid> {
    let mut oids = Vec::new();

    for mat in OID_REGEX.find_iter(message) {
        if let Ok(object) = src.find_object_by_prefix(mat.as_str(), None) {
            oids.push(object.id());
        }
    }

    oids
}

pub fn replace_oids(repo: &Repository, src_message: &str, tr_map: &HashMap<Oid, Oid>) -> String {
    let mut result = src_message.to_string();

    for mat in OID_REGEX.find_iter(src_message) {
        let prefix = mat.as_str();
        if let Ok(object) = repo.find_object_by_prefix(prefix, None) {
            let full_oid = object.id();
            if let Some(new_oid) = tr_map.get(&full_oid) {
                let new_oid_str = &new_oid.to_string()[..prefix.len()];
                result.replace_range(mat.start()..mat.end(), new_oid_str);
            }
        }
    }

    result
}

#[cfg(test)]
mod test_oids {
    use super::*;
    use git2::{Commit, ObjectType, Oid, Repository};
    use std::collections::HashMap;
    use tempdir::TempDir;

    fn create_temp_repo() -> (TempDir, Repository) {
        let dir = TempDir::new("test_repo").unwrap();
        let repo = Repository::init(dir.path()).unwrap();
        (dir, repo)
    }

    fn create_commit(repo: &Repository, message: &str) -> Oid {
        let sig = repo.signature().unwrap();
        let tree_oid = {
            let mut index = repo.index().unwrap();
            let tree_oid = index.write_tree().unwrap();
            repo.find_tree(tree_oid).unwrap()
        };
        let parent_commit = None;
        repo.commit(Some("HEAD"), &sig, &sig, message, &tree_oid, parent_commit)
            .unwrap()
    }

    #[test]
    fn test_extract_oids() {
        let (_dir, repo) = create_temp_repo();
        let commit_oid = create_commit(&repo, "Initial commit");
        let message = format!(
            "This reverts commit {} and cherry-picks from def567.",
            &commit_oid.to_string()[..6]
        );
        let oids = extract_oids(&repo, &message);
        assert_eq!(oids.len(), 1);
        assert_eq!(oids[0], commit_oid);
    }

    #[test]
    fn test_replace_oids() {
        let (_dir, repo) = create_temp_repo();
        let commit_oid1 = create_commit(&repo, "First commit");
        let commit_oid2 = create_commit(&repo, "Second commit");
        let message = format!(
            "This reverts commit {} and cherry-picks from {}.",
            &commit_oid1.to_string()[..6],
            &commit_oid2.to_string()[..6]
        );

        let mut tr_map = HashMap::new();
        let new_oid1 = Oid::from_str("1234567890abcdef1234567890abcdef12345678").unwrap();
        let new_oid2 = Oid::from_str("abcdefabcdefabcdefabcdefabcdefabcdefabcdef").unwrap();
        tr_map.insert(commit_oid1, new_oid1);
        tr_map.insert(commit_oid2, new_oid2);

        let new_message = replace_oids(&repo, &message, tr_map);
        assert!(new_message.contains("123456"));
        assert!(new_message.contains("abcdef"));
        assert!(!new_message.contains(&commit_oid1.to_string()[..6]));
        assert!(!new_message.contains(&commit_oid2.to_string()[..6]));
    }
}
