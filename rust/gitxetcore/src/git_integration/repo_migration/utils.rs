use git2::{Oid, Repository};
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::{HashMap, HashSet};

// Detects OIDs stored in commit messages like merges.  Minimum 6 characters.
lazy_static! {
    // Match between 6 and 40 characters with non-hex characters or line endings on either side
    static ref OID_REGEX: Regex = Regex::new(r"(?:^|[^0-9a-f])([0-9a-f]{6,40})(?:[^0-9a-f]|$)").unwrap();
}

/// Extracts all the valid source oids present in a message.
pub fn extract_str_oids(src: &Repository, message: &str) -> Vec<Oid> {
    let mut oids = Vec::new();

    for cap in OID_REGEX.captures_iter(message) {
        if let Some(mat) = cap.get(1) {
            if let Ok(object) = src.find_object_by_prefix(mat.as_str(), None) {
                oids.push(object.id());
            }
        }
    }

    oids
}

/// Commit messages track Ids by saying
pub fn replace_oids(repo: &Repository, src_message: &str, tr_map: &HashMap<Oid, Oid>) -> String {
    let mut result = src_message.to_string();

    for cap in OID_REGEX.captures_iter(src_message) {
        if let Some(mat) = cap.get(1) {
            let prefix = mat.as_str();
            if let Ok(object) = repo.find_object_by_prefix(prefix, None) {
                let full_oid = object.id();
                if let Some(new_oid) = tr_map.get(&full_oid) {
                    let new_oid_str = &new_oid.to_string()[..prefix.len()];
                    result.replace_range(mat.start()..mat.end(), new_oid_str);
                }
            }
        }
    }

    result
}

#[allow(unused)]
pub(crate) fn find_roots_and_detect_cycles(
    graph: &HashMap<Oid, HashSet<Oid>>,
) -> (HashSet<Oid>, bool) {
    let mut roots = HashSet::new();
    let mut has_cycle = false;

    // Colors: 0 = white (unvisited), 1 = gray (visiting), 2 = black (visited)
    let mut colors: HashMap<Oid, u8> = HashMap::new();

    for &oid in graph.keys() {
        colors.insert(oid, 0); // Initialize all nodes to white
    }

    // Identify potential roots (nodes not in the map or nodes with empty dependents)
    for &oid in graph.keys() {
        if graph[&oid].is_empty() {
            roots.insert(oid);
        }
    }

    // Helper function to perform DFS iteratively
    fn dfs(
        node: Oid,
        graph: &HashMap<Oid, HashSet<Oid>>,
        colors: &mut HashMap<Oid, u8>,
        roots: &mut HashSet<Oid>,
    ) -> bool {
        let mut stack = Vec::new();
        stack.push((node, false)); // (current node, is node processed)

        while let Some((current, processed)) = stack.pop() {
            if processed {
                colors.insert(current, 2); // Mark as black (visited)
            } else {
                match colors.get(&current).cloned() {
                    Some(0) => {
                        // White
                        colors.insert(current, 1); // Mark as gray (visiting)
                        stack.push((current, true)); // Mark as processed after children
                        if let Some(dependents) = graph.get(&current) {
                            for &dep in dependents {
                                stack.push((dep, false)); // Push children to stack
                            }
                        } else {
                            roots.insert(current); // If no dependents, it's a root
                        }
                    }
                    Some(1) => return true, // Gray (visiting) -> cycle detected
                    Some(2) => continue,    // Black (visited) -> skip
                    Some(_) => {
                        debug_assert!(false);
                    }
                    None => {
                        roots.insert(current);
                    } // Node not in graph, it's a root
                }
            }
        }

        false
    }

    for &oid in graph.keys() {
        if colors[&oid] == 0 {
            // Unvisited
            if dfs(oid, graph, &mut colors, &mut roots) {
                has_cycle = true;
            }
        }
    }

    (roots, has_cycle)
}

#[cfg(test)]
mod test_oids {
    use super::*;
    use git2::{Oid, Repository};
    use std::collections::HashMap;
    use tempdir::TempDir;

    fn create_temp_repo() -> (TempDir, Repository) {
        let dir = TempDir::new("test_repo").unwrap();
        let repo = Repository::init(dir.path()).unwrap();
        (dir, repo)
    }

    fn create_commit(repo: &Repository, message: &str, parents: Vec<Oid>) -> Oid {
        let sig = repo.signature().unwrap();
        let tree_oid = {
            let mut index = repo.index().unwrap();
            let tree_oid = index.write_tree().unwrap();
            repo.find_tree(tree_oid).unwrap()
        };
        let parent_commits = parents
            .into_iter()
            .map(|p| repo.find_commit(p).unwrap())
            .collect::<Vec<_>>();

        repo.commit(
            Some("HEAD"),
            &sig,
            &sig,
            message,
            &tree_oid,
            &parent_commits.iter().collect::<Vec<_>>()[..],
        )
        .unwrap()
    }

    #[test]
    fn test_extract_oids() {
        let (_dir, repo) = create_temp_repo();
        let commit_oid = create_commit(&repo, "Initial commit", vec![]);

        // Note: def567 not in repo, so should be ignored.:wa
        let message = format!(
            "This reverts commit {} and cherry-picks from def567.",
            &commit_oid.to_string()[..6]
        );
        let oids = extract_str_oids(&repo, &message);
        assert_eq!(oids.len(), 1);
        assert_eq!(oids[0], commit_oid);
    }

    #[test]
    fn test_replace_oids() {
        let (_dir, repo) = create_temp_repo();
        let commit_oid1 = create_commit(&repo, "First commit", vec![]);
        let commit_oid2 = create_commit(&repo, "Second commit", vec![commit_oid1]);
        let message = format!(
            "This reverts commit {} and cherry-picks from {}.",
            &commit_oid1.to_string()[..6],
            &commit_oid2.to_string()[..6]
        );

        let mut tr_map = HashMap::new();
        let new_oid1 = Oid::from_str("1234567890abcdef1234567890abcdef12345678").unwrap();
        let new_oid2 = Oid::from_str("abcdefabcdefabefabcdefabcdefabcdefabcdef").unwrap();
        tr_map.insert(commit_oid1, new_oid1);
        tr_map.insert(commit_oid2, new_oid2);

        let new_message = replace_oids(&repo, &message, &tr_map);
        assert!(new_message.contains("123456"));
        assert!(new_message.contains("abcdef"));
        assert!(!new_message.contains(&commit_oid1.to_string()[..6]));
        assert!(!new_message.contains(&commit_oid2.to_string()[..6]));
    }
}
