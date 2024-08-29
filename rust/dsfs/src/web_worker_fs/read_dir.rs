use std::{
    collections::VecDeque,
    ffi::OsString,
    io,
    path::{Path, PathBuf},
};

use web_sys::{
    js_sys::{Array, AsyncIterator, IteratorNext, JsString},
    wasm_bindgen::JsValue,
    FileSystemFileHandle, FileSystemHandle,
};

use crate::log;

use super::{futurize, get_directory, Metadata};

pub struct AsyncReadDir {
    path: PathBuf,
    inner: AsyncIterator,
    done: bool,
}

impl AsyncReadDir {
    fn new(path: PathBuf, inner: AsyncIterator) -> Self {
        AsyncReadDir {
            path,
            inner,
            done: false,
        }
    }

    async fn get_next(&mut self) -> Option<DirEntry> {
        if self.done {
            return None;
        }
        let next_promise = self.inner.next().ok()?;
        let next: IteratorNext = futurize(next_promise).await.ok()?;
        if next.done() {
            self.done = true;
            return None;
        }
        let value: Array = next.value().into();
        let name: String = JsString::from(value.get(0)).into();
        let handle: FileSystemHandle = value.get(1).into();
        let inner: DirEntryInner = match handle.kind() {
            web_sys::FileSystemHandleKind::File => {
                let file_handle = FileSystemFileHandle::from(JsValue::from(handle));
                let file = futurize(file_handle.get_file()).await.ok()?;
                DirEntryInner::File(file)
            }
            web_sys::FileSystemHandleKind::Directory => DirEntryInner::Directory,
            _ => {
                log!("weird exception, handle isn't directory or file");
                return None;
            }
        };

        let mut path = self.path.clone();
        path.push(&name);
        Some(DirEntry::new(inner, name, path))
    }
}

#[async_trait::async_trait]
pub trait DSAsyncIterator {
    type Item;
    fn next(&mut self) -> impl std::future::Future<Output = Option<Self::Item>>;
}

#[async_trait::async_trait]
impl DSAsyncIterator for AsyncReadDir {
    type Item = DirEntry;

    fn next(&mut self) -> impl std::future::Future<Output = Option<Self::Item>> {
        self.get_next()
    }
}

#[derive(Debug, Clone)]
enum DirEntryInner {
    File(web_sys::File),
    Directory,
}

#[derive(Debug, Clone)]
pub struct DirEntry {
    inner: DirEntryInner,
    name: String,
    path: PathBuf,
}

impl DirEntry {
    fn new(inner: DirEntryInner, name: String, path: PathBuf) -> Self {
        Self { inner, name, path }
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn file_name(&self) -> OsString {
        self.name.clone().into()
    }

    pub fn metadata(&self) -> io::Result<Metadata> {
        Ok(match &self.inner {
            DirEntryInner::File(f) => Metadata::new_file(f.clone()),
            DirEntryInner::Directory => Metadata::new_directory(),
        })
    }
}

pub async fn read_dir_async<P: AsRef<Path>>(path: P) -> io::Result<AsyncReadDir> {
    let dir = get_directory(path.as_ref(), false).await?;
    let inner = dir.entries();
    Ok(AsyncReadDir::new(path.as_ref().to_path_buf(), inner))
}

pub async fn read_dir<P: AsRef<Path>>(path: P) -> io::Result<ReadDir> {
    let ard = read_dir_async(path).await?;
    Ok(ReadDir::from_async(ard).await)
}

pub struct ReadDir {
    inner: VecDeque<DirEntry>,
}

impl Iterator for ReadDir {
    type Item = DirEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.pop_front()
    }
}

impl ReadDir {
    pub async fn from_async(mut rd: AsyncReadDir) -> Self {
        let mut inner = VecDeque::new();
        while let Some(dirent) = rd.next().await {
            inner.push_back(dirent);
        }
        ReadDir { inner }
    }
}
