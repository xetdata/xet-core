use base64;

use git2::{Repository, Signature};
use std::sync::Arc;

use std::path::Path;
use tracing::error;

use crate::config::XetConfig;

use super::git_repo_plumbing::open_libgit2_repo;
use super::git_user_config::get_repo_signature;

pub struct GitNotesWrapper {
    repo: Arc<Repository>,
    notes_ref: String,
    write_signature: Signature<'static>,
}

unsafe impl Send for GitNotesWrapper {}
unsafe impl Sync for GitNotesWrapper {}

/// A wrapper around git notes that provides storage of arbitrary blobs
/// into notes.
///
/// A note is a mapping from An "Annotated Oid" ==> "Blob Oid"
///
/// 1) Typically this "Annotated Oid" is some other object / commit id.
///
/// 2) The Blob contents, while theoretically can be any arbitrary
/// byte string, the libgit2 C API *requires* this string to be a C string,
/// and the rust API then requires it to be utf-8. (Interestingly, the raw git
/// commandline actually does not require this).
///
/// (1) can be a little limiting as it may be hard to always find an Annotated
/// ID to use. We could in the post-commit hook for instance, find the HEAD
/// commit id. But this may not be resilient to other stranger use cases.
///
/// we take advantage of the fact that this "Annotated Oid" does not
/// actually have to exist. i.e. we can just put really any random number into
/// it. To have this work consistently , we will actually use a hash of the
/// *contents* of the blob as the annotated Oid. The advantage here is that
/// we do not have to worry about overwriting existing blobs; i.e. if you have
/// to overwrite, it is an identical blob.
///
/// For (2), unfortunately this means I actually need to force convert
/// a &[u8] into something utf-8-ish unless we fork and patch:
///  -libgit
///  -libgit2-rs
///
/// Originally I considered the option of storing the blob seperately, and
/// just writing a reference to the blob in the note. But this means the actual
/// blob is not in the tree and doesn't get pushed.
///
///
/// So the result is in pseudo-code
///
/// add_note(bytes) {
///    blob_oid = git_add_blob(escape(bytes))
///    create note mapping blob_oid -> escape(blob_oid)
/// }
///
///
/// iterator() {
///    for each note (annotated_id, note_oid) {
///        bytes = unescape(read annotated_oid)
///        yield bytes
///    }
/// }
impl GitNotesWrapper {
    /// Open will automatically try to the find a repository
    /// in the path, moving up the directory tree as needed
    pub fn open<P: AsRef<Path>>(
        path: P,
        config: &XetConfig,
        notes_ref: &str,
    ) -> Result<GitNotesWrapper, git2::Error> {
        let repo = open_libgit2_repo(Some(path.as_ref()))?;
        Self::from_repo(repo, config, notes_ref)
    }

    pub fn from_repo(
        repo: Arc<Repository>,
        config: &XetConfig,
        notes_ref: &str,
    ) -> Result<GitNotesWrapper, git2::Error> {
        Ok(GitNotesWrapper {
            repo: repo.clone(),
            notes_ref: notes_ref.into(),
            write_signature: get_repo_signature(Some(config), None, Some(repo.clone())),
        })
    }

    /// Returns an iterator over git blobs
    /// Example:
    ///
    /// ```ignore
    /// for (oid, blob) in repo.notes_content_iterator() {
    ///    // oid is a git Oid as a String
    ///    // blob is a Vec<u8>
    /// }
    /// ```
    ///
    #[allow(clippy::type_complexity)]
    pub fn notes_content_iterator<'a>(
        &'a self,
    ) -> Result<Arc<tokio::sync::Mutex<dyn Iterator<Item = (String, Vec<u8>)> + 'a>>, git2::Error>
    {
        let notes_result = self.repo.notes(Some(&self.notes_ref));

        match notes_result {
            Ok(notes) => Ok(Arc::new(tokio::sync::Mutex::new(
                notes
                    .flatten()
                    .flat_map(|(_, annotated_id)| {
                        self.repo
                            .find_blob(annotated_id)
                            .map(|b| (annotated_id.to_string(), b))
                    })
                    .flat_map(|(id, blob)| {
                        let ret = base64::decode(blob.content()).map(|b| (id, b));
                        if ret.is_err() {
                            error!("Unable to decode blob {:?}", blob.id());
                        }
                        ret
                    }),
            ))),
            Err(e) => {
                // its ok if the entry is not found.
                // This means a new note ref. So we
                // We return an empty iterator.
                if e.code() == git2::ErrorCode::NotFound {
                    Ok(Arc::new(tokio::sync::Mutex::new(std::iter::empty())))
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Return an interator over note names
    /// (1st tuple field in notes_content_iterator())
    ///
    /// note_name_to_content() can be used to get the content of the note
    #[allow(clippy::type_complexity)]
    pub fn notes_name_iterator<'a>(
        &'a self,
    ) -> Result<Box<dyn Iterator<Item = String> + 'a>, git2::Error> {
        let notes_result = self.repo.notes(Some(&self.notes_ref));

        match notes_result {
            Ok(notes) => Ok(Box::new(
                notes
                    .flatten()
                    .map(|(_, annotated_id)| annotated_id.to_string()),
            )),
            Err(e) => {
                // its ok if the entry is not found.
                // This means a new note ref. So we
                // We return an empty iterator.
                if e.code() == git2::ErrorCode::NotFound {
                    Ok(Box::new(std::iter::empty()))
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Return the content of a note with a give name.
    #[allow(clippy::type_complexity)]
    pub fn notes_name_to_content(&self, name: &str) -> Result<Vec<u8>, git2::Error> {
        let oid = git2::Oid::from_str(name)?;
        let blob = self.repo.find_blob(oid)?;
        if let Ok(ret) = base64::decode(blob.content()) {
            Ok(ret)
        } else {
            error!("Unable to decode blob {:?}", blob.id());
            Err(git2::Error::new(
                git2::ErrorCode::Invalid,
                git2::ErrorClass::Object,
                "Unable to decode blob as base64",
            ))
        }
    }

    /// Adds some content into the repository. This can be read out later
    /// via notes_content_iterator()
    pub fn add_note<T: AsRef<[u8]>>(&self, content: T) -> Result<(), git2::Error> {
        let content_str = base64::encode(content.as_ref());
        let blob_oid = self.repo.blob(content_str.as_bytes())?;
        let note_ok = self.repo.note(
            &self.write_signature,
            &self.write_signature,
            Some(&self.notes_ref),
            blob_oid,
            &content_str,
            false,
        );
        match note_ok {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.code() == git2::ErrorCode::Exists {
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Checks if a certain note already exists.
    pub fn find_note<T: AsRef<[u8]>>(&self, content: T) -> Result<bool, git2::Error> {
        let content_str = base64::encode(content.as_ref());
        let blob_oid = git2::Oid::hash_object(git2::ObjectType::Blob, content_str.as_bytes())?;

        Ok(self.repo.find_note(Some(&self.notes_ref), blob_oid).is_ok())
    }
}
