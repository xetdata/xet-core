use base64;

use tracing::{error, info};

use super::git_repo::Git2RepositoryReadGuard;
use crate::errors::Result;

pub struct GitNotesIteratorWrapper<'a> {
    pub read_guard: Git2RepositoryReadGuard<'a>,
    pub note_iter: git2::Notes<'a>,
}

impl<'a> GitNotesIteratorWrapper<'a> {
    fn next(&mut self) -> Option<(git2::Oid, git2::Oid)> {
        loop {
            match self.note_iter.next() {
                None => {
                    return None;
                }
                Some(Err(e)) => {
                    if e.code() != git2::ErrorCode::NotFound {
                        error!("Error extracting note: {e:?}");
                    }
                    continue;
                }
                Some(Ok(items)) => {
                    return Some(items);
                }
            };
        }
    }
}

pub struct GitNotesContentIterator<'a> {
    pub notes: Option<GitNotesIteratorWrapper<'a>>,
}

impl<'a> Iterator for GitNotesContentIterator<'a> {
    type Item = Result<(String, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let Some(gniw) = self.notes.as_mut() else {
                return None;
            };

            let res = gniw.next();

            let Some((_, annotated_id)) = res else {
                // Just have to change the type...
                return None;
            };

            let Ok(blob) = gniw.read_guard.find_blob(annotated_id) else {
                info!("Error: No blob found for notes oid {annotated_id:?}");
                continue;
            };

            let id_s = annotated_id.to_string();

            let Ok(ret) = base64::decode(blob.content()).map_err(|e| {
                error!("Error decoding blob {:?}", blob.id());
                e
            }) else {
                continue;
            };

            return Some(Ok((id_s, ret)));
        }
    }
}

pub struct GitNotesNameIterator<'a> {
    pub notes: Option<GitNotesIteratorWrapper<'a>>,
}

impl<'a> Iterator for GitNotesNameIterator<'a> {
    type Item = git2::Oid;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(gniw) = self.notes.as_mut() {
                gniw.next().map(|(_, an_oid)| an_oid);
            }
        }
    }
}
