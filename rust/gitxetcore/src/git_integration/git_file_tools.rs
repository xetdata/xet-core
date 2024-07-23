use crate::errors::Result;
use crate::git_integration::git_process_wrapping;
use std::path::PathBuf;

use tracing::{error, warn};

#[derive(Default)]
pub struct GitTreeListingEntry {
    pub object_id: String,
    pub path: String,
    pub permissions: u32,
    pub size: u64,
}

pub struct GitTreeListing {
    pub base_dir: PathBuf,
    pub sub_directories: Vec<GitTreeListingEntry>,
    pub files: Vec<GitTreeListingEntry>,
}

impl GitTreeListing {
    /// List all the files in the repository,
    ///
    /// Use PathBuf::default() for the subdir to run in the base directory, and use "HEAD" or HEAD for the current HEAD.
    ///
    pub fn build(
        base_dir: &PathBuf,
        ref_id: Option<&str>,
        recursive: bool,
        files_only: bool,
        fill_size: bool,
    ) -> Result<Self> {
        let mut args: Vec<&str> = vec!["-z"];
        if recursive {
            args.push("-r");
        }

        if fill_size {
            args.push("-l");
        }

        args.push(ref_id.unwrap_or("HEAD"));

        let mut ret = Self {
            base_dir: base_dir.clone(),
            sub_directories: Vec::new(),
            files: Vec::new(),
        };

        let (_, output, _) = git_process_wrapping::run_git_captured(
            Some(base_dir),
            "ls-tree",
            &args[..],
            true,
            None,
        )?;

        #[derive(PartialEq)]
        enum ObjType {
            Blob,
            Tree,
            Commit,
        }

        {
            // This splits on both the newlines and the spaces / tabs in the output.
            // Format is always
            //
            //   object_permissions object_type object_id\tpath\n
            //
            // So this output below should solidly be able to iterate through it all.

            // Helper function to get the next field by whitespace.
            let next_field =
                |idx: &mut usize, search_char: char, allow_multiple: bool| -> Option<&str> {
                    let start_index = *idx;

                    // See if we're done.
                    if start_index + 1 >= output.len() {
                        return None;
                    }
                    // Search forward.
                    let next_index = match &output[start_index..].find(search_char) {
                        Some(idx) => idx + start_index,
                        None => output.len(),
                    };

                    // Find the next index that isn't a whitespace character to form the starting point
                    // of the next search term.
                    if allow_multiple {
                        *idx = if next_index + 1 < output.len() {
                            match &output[next_index..].find(|c: char| c != search_char) {
                                Some(idx) => idx + next_index,
                                None => output.len(),
                            }
                        } else {
                            output.len()
                        };
                    } else {
                        *idx = usize::min(next_index + 1, output.len());
                    }

                    Some(&output[start_index..next_index])
                };

            // Event loop to parse the input.

            let mut line_number = 0;
            let mut idx = 0;
            loop {
                let line_start_idx = idx;

                let print_parse_error = || {
                    error!(
                        "Premature end in ls-trees output line {:?}: {}",
                        line_number,
                        &output[line_start_idx..]
                    );
                };

                let mut entry = GitTreeListingEntry::default();

                // Ignore the file permissions
                if let Some(file_perm) = next_field(&mut idx, ' ', false) {
                    debug_assert!(file_perm.chars().all(|c| c.is_numeric()));
                    entry.permissions = match u32::from_str_radix(file_perm, 8) {
                        Ok(v) => v,
                        Err(e) => {
                            error!(
                                "Invalid file permissions string: {} (Error: {:?})",
                                &file_perm, &e
                            );
                            0
                        }
                    };
                } else {
                    // An expected end
                    break;
                }

                // Determine the type of the file
                let (obj_type, use_entry) = match next_field(&mut idx, ' ', false) {
                    Some("blob") => (ObjType::Blob, true),
                    Some("tree") => (ObjType::Tree, !files_only),
                    Some("commit") => {
                        error!(
                            "Parsing Error in ls-trees output line, unexpected type \"commit\": {:?}, {}",
                            line_number, &output[line_start_idx..]
                        );
                        (ObjType::Commit, false)
                    }
                    _ => {
                        print_parse_error();
                        break;
                    }
                };

                // Go to end and discard the rest.
                if !use_entry {
                    let _ = next_field(&mut idx, '\n', false);
                    line_number += 1;
                    continue;
                }

                // Next is object ID.  Multiple spaces after this depending on
                if let Some(s) = next_field(&mut idx, if fill_size { ' ' } else { '\t' }, fill_size)
                {
                    s.clone_into(&mut entry.object_id);
                } else {
                    print_parse_error();
                    break;
                }

                if fill_size {
                    if let Some(s) = next_field(&mut idx, '\t', false) {
                        entry.size = if obj_type == ObjType::Blob {
                            s.parse::<u64>().unwrap_or(0)
                        } else {
                            0
                        };
                    } else {
                        print_parse_error();
                        break;
                    }
                }

                // Next is the path.  This is from the query directory.
                if let Some(s) = next_field(&mut idx, '\0', false) {
                    s.clone_into(&mut entry.path);
                } else {
                    print_parse_error();
                    break;
                }

                match obj_type {
                    ObjType::Blob => ret.files.push(entry),
                    ObjType::Tree => {
                        if !files_only {
                            debug_assert!(use_entry);
                            ret.sub_directories.push(entry)
                        } else {
                            debug_assert!(!use_entry);
                        }
                    }
                    _ => {}
                }
                line_number += 1;
            }
        }
        Ok(ret)
    }
}

/// Translates git encoded file names or other strings to their true unicode versions.
///
/// Git uses heavily escaped file naming conventions when passing file name strings through stdin/stdout.
/// Any filename containing unicode characters or others like \t causes the name to be put in quotes, and
/// everything not in the regular alphanumeric to be escaped.  Unicode bytes are escaped as \XXX, where XXX is
/// the octal value of the byte.
///
/// For example, the filename
///
/// ûnícõdé
///
/// will come through the git stdin/stdout stream as
///
/// ""\303\273n\303\255c\303\265d\303\251""
///
/// This method translates the latter into the former.
pub fn decode_git_string(s_in: &str) -> String {
    // Is the first character a "?  If not, it doesn't need translation.
    if !s_in.starts_with('\"') {
        return s_in.to_owned();
    }

    // Are there any actually unicode characters here?
    let mut i = match s_in.find('\\') {
        Some(idx) => idx,
        None => {
            // Nothing needs escaping
            return s_in.to_owned();
        }
    };

    let bytes_in = s_in.as_bytes();
    let mut s_unicode_converted: Vec<u8> = Vec::with_capacity(bytes_in.len());
    s_unicode_converted.extend_from_slice(&bytes_in[..i]);

    while i < bytes_in.len() {
        if i + 4 <= bytes_in.len()
            && unsafe {
                *bytes_in.get_unchecked(i) == b'\\'
                    && (*bytes_in.get_unchecked(i + 1) >= b'0'
                        && *bytes_in.get_unchecked(i + 1) <= b'9')
                    && (*bytes_in.get_unchecked(i + 2) >= b'0'
                        && *bytes_in.get_unchecked(i + 2) <= b'9')
                    && (*bytes_in.get_unchecked(i + 3) >= b'0'
                        && *bytes_in.get_unchecked(i + 3) <= b'9')
            }
        {
            match u8::from_str_radix(
                unsafe { std::str::from_utf8_unchecked(&bytes_in[(i + 1)..(i + 4)]) },
                8,
            ) {
                Ok(n) => s_unicode_converted.push(n),
                Err(_) => s_unicode_converted.extend_from_slice(&bytes_in[i..(i + 4)]),
            }

            i += 4;
            continue;
        } else {
            s_unicode_converted.push(bytes_in[i]);
            i += 1;
        }
    }

    let s_utf8 = match std::str::from_utf8(&s_unicode_converted[..]) {
        Ok(s) => s,
        Err(e) => {
            warn!("Error decoding converted string into UTF8: {:?}", &e);
            match std::str::from_utf8(bytes_in) {
                Ok(s) => s,
                Err(e) => {
                    warn!("Error decoding raw git string into UTF8: {:?}", &e);
                    s_in
                }
            }
        }
    };

    // this unescapes all the rest of the \t, \\, etc.
    if s_utf8.len() <= 1 {
        s_utf8.to_owned()
    } else {
        match shellish_parse::parse(&s_utf8[1..], false) {
            Ok(s) => {
                if !s.is_empty() {
                    s[0].clone()
                } else {
                    s_utf8.to_owned()
                }
            }
            Err(e) => {
                warn!(
                    "Error interpreting raw git string {} from git: {:?}.",
                    &s_utf8, e
                );
                s_utf8.to_owned()
            }
        }
    }
}

#[cfg(test)]
mod git_file_tools_tests {

    use itertools::Itertools;

    use super::*;

    use crate::git_integration::git_xet_repo::git_repo_test_tools::TestRepo;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_listing() -> Result<()> {
        let tr = TestRepo::new()?;

        tr.test_consistent()?;

        let files = vec!["test_file_1.dat".to_owned(), "test_file_2.dat".to_owned()];

        // Test to make sure the cleanliness of the repository is correctly reported.
        for f in files.iter() {
            tr.write_file(f, 0, 100)?;
            tr.repo.run_git_checked_in_repo("add", &[f])?;
        }

        tr.repo
            .run_git_checked_in_repo("commit", &["-m", "Added test_files_*.dat"])?;

        let out_list = |ref_name, recursive| -> Result<Vec<String>> {
            Ok(
                GitTreeListing::build(&tr.repo.repo_dir, ref_name, recursive, true, true)?
                    .files
                    .into_iter()
                    .map(|e| {
                        assert_eq!(e.size, 100);
                        e.path
                    })
                    .sorted()
                    .collect(),
            )
        };

        assert_eq!(out_list(None, false)?, files);

        for f in files.iter() {
            tr.repo.run_git_checked_in_repo("rm", &[f])?;
        }

        // This should be from the tree, not the directory.
        assert_eq!(out_list(None, false)?, files);

        tr.repo
            .run_git_checked_in_repo("commit", &["-m", "Removed test_files_*.dat"])?;

        assert!(out_list(None, false)?.is_empty());

        assert_eq!(out_list(Some("HEAD~1"), false)?, files);

        let subfiles = vec!["foo/bar_1.dat".to_owned(), "foo/bar_2.dat".to_owned()];

        tr.write_file(&subfiles[0], 0, 100)?;
        tr.write_file(&subfiles[1], 0, 100)?;
        tr.repo.run_git_checked_in_repo("add", &["foo/"])?;

        tr.repo
            .run_git_checked_in_repo("commit", &["-m", "added foo"])?;

        assert!(out_list(None, false)?.is_empty());
        assert_eq!(out_list(None, true)?, subfiles);

        let extract_paths = |v: Vec<GitTreeListingEntry>| -> Vec<String> {
            v.into_iter()
                .map(|e| {
                    assert_eq!(e.size, 0); // Not querying the size
                    e.path
                })
                .sorted()
                .collect()
        };

        let base_list = GitTreeListing::build(&tr.repo.repo_dir, None, false, false, false)?;

        // Should just show the subdirectory here
        assert!(base_list.files.is_empty());

        assert_eq!(
            extract_paths(base_list.sub_directories),
            vec!["foo".to_owned()]
        );

        let subdir_list =
            GitTreeListing::build(&(tr.repo.repo_dir.join("foo/")), None, false, true, false)?;

        assert!(subdir_list.sub_directories.is_empty());

        assert_eq!(
            extract_paths(subdir_list.files),
            vec!["bar_1.dat".to_owned(), "bar_2.dat".to_owned()]
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[cfg(unix)] // Certain file names below contain forbidden characters
    async fn test_listing_odd_names() -> Result<()> {
        let tr = TestRepo::new()?;

        tr.test_consistent()?;

        let files: Vec<_> = vec![
            "ûnícõdé".to_owned(),
            "unicode_filename_⁴⨍⤫Ⳳ➢⻋◺❿₦⽞⎖⏧".to_owned(),
            "my strange file 1.dat".to_owned(),
            "another\tstrange\tfile.dat".to_owned(),
            "_".to_owned(),
            "\"".to_owned(),
            "\n".to_owned(),
            " ".to_owned(),
            "\t".to_owned(),
            "\\backslash\\\\".to_owned(),
            "⨍".to_owned(),
        ]
        .into_iter()
        .sorted()
        .collect();

        // Test to make sure the cleanliness of the repository is correctly reported.
        for f in files.iter() {
            tr.write_file(f, 0, 100)?;
            tr.repo.run_git_checked_in_repo("add", &[f])?;
        }

        tr.repo
            .run_git_checked_in_repo("commit", &["-m", "Added test_files_*.dat"])?;

        let out_list = |ref_name, recursive| -> Result<Vec<String>> {
            Ok(
                GitTreeListing::build(&tr.repo.repo_dir, ref_name, recursive, true, true)?
                    .files
                    .into_iter()
                    .map(|e| {
                        assert_eq!(e.size, 100);
                        e.path
                    })
                    .sorted()
                    .collect(),
            )
        };

        assert_eq!(out_list(None, false)?, files);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_decode_git_string() -> Result<()> {
        // let strange_file_name = r#""\303\273n\303\255c\303\265d\303\251"#;
        let strange_file_name = r#""another\tstrange\tfile.dat"#;
        assert_eq!(
            decode_git_string(strange_file_name),
            String::from("another\tstrange\tfile.dat")
        );
        let strange_file_name = r#""\\backslash\\\\"#;
        assert_eq!(
            decode_git_string(strange_file_name),
            String::from("\\backslash\\\\")
        );
        Ok(())
    }
}
