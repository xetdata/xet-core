use std::io::{Error, ErrorKind, SeekFrom};
use std::path::{Component, PathBuf};
use std::sync::Mutex;
use std::{io, path::Path};

use normalize_path::NormalizePath;
pub use web_sys::console;
use web_sys::js_sys::Promise;
use web_sys::wasm_bindgen::prelude::*;
use web_sys::{
    DedicatedWorkerGlobalScope, FileSystemDirectoryHandle, FileSystemFileHandle,
    FileSystemGetDirectoryOptions, FileSystemGetFileOptions, FileSystemReadWriteOptions,
    FileSystemRemoveOptions, FileSystemSyncAccessHandle,
};

mod metadata;
mod read_dir;
pub use metadata::*;
pub use read_dir::*;

// A macro to provide `println!(..)`-style syntax for `console.log` logging.
#[macro_export]
macro_rules! log {
    ( $( $t:tt )* ) => {
        $crate::console::log_1(&format!( $( $t )* ).into());
    }
}

#[macro_export]
macro_rules! trace {
    ( $( $t:tt )* ) => {
        $crate::console::trace_1(&format!( $( $t )* ).into());
    }
}

pub async fn create_dir_all<P: AsRef<Path>>(path: P) -> io::Result<()> {
    get_directory(path, true).await?;

    Ok(())
}

async fn get_directory<P: AsRef<Path>>(
    path: P,
    create: bool,
) -> Result<FileSystemDirectoryHandle, Error> {
    let path = path.as_ref().normalize();
    let root_dir = get_root_directory().await?;
    let mut current = root_dir;

    let options = FileSystemGetDirectoryOptions::new();
    options.set_create(create);
    for component in path.components() {
        match component {
            Component::Normal(dir) => {
                let sdir = dir.to_str().ok_or(Error::new(ErrorKind::Other, "no dir"))?;
                let next =
                    futurize(current.get_directory_handle_with_options(sdir, &options)).await?;
                current = next;
            }
            Component::RootDir => continue,
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                return Err(Error::new(ErrorKind::Other, "impossible"));
            }
        }
    }

    Ok(current)
}

async fn get_file<P: AsRef<Path>>(path: P, create: bool) -> Result<FileSystemFileHandle, Error> {
    let path = path.as_ref().normalize();
    let file_name = path.file_name().unwrap().to_str().unwrap();
    let dir_path = path.parent().unwrap();
    let dir = get_directory(dir_path, create).await?;

    let options = FileSystemGetFileOptions::new();
    options.set_create(create);
    let file: FileSystemFileHandle =
        futurize(dir.get_file_handle_with_options(file_name, &options)).await?;

    Ok(file)
}

// look into FileSystemWritableFileStream
// let w: FileSystemWritableFileStream = futurize(file.create_writable()).await?; // TODO error handling

pub async fn write<P: AsRef<Path>, C: AsRef<[u8]>>(path: P, contents: C) -> io::Result<()> {
    trace!("AVtrace123write direct: {:?}", path.as_ref());
    let file = get_file(path, false).await?;

    let access_handle = get_access_handle(&file).await?;
    // TODO: figure out if need to specify at param https://developer.mozilla.org/en-US/docs/Web/API/FileSystemSyncAccessHandle/write#options
    let c = contents.as_ref();
    let size = c.len();
    let _ = access_handle
        .write_with_u8_array(contents.as_ref())
        .map_err(js_val_to_io_error)?;
    let mut num_written: usize = 0;
    while num_written < size {
        num_written += access_handle
            .write_with_u8_array(&c[num_written..])
            .map_err(js_val_to_io_error)? as usize;
    }
    Ok(())
}

async fn get_access_handle(
    file: &FileSystemFileHandle,
) -> Result<FileSystemSyncAccessHandle, io::Error> {
    let access_handle: FileSystemSyncAccessHandle =
        futurize(file.create_sync_access_handle()).await?;
    Ok(access_handle)
}

pub async fn read<P: AsRef<Path>>(path: P) -> io::Result<Vec<u8>> {
    trace!("AVtrace123read direct: {:?}", path.as_ref());
    let file = get_file(path, false).await?;
    let access_handle = get_access_handle(&file).await?;
    let size = access_handle.get_size().unwrap() as usize;
    let mut res = Vec::with_capacity(size);
    let mut num_read: usize = 0;
    while num_read < size {
        num_read += access_handle
            .read_with_u8_array(&mut res[num_read..])
            .map_err(js_val_to_io_error)? as usize;
    }

    Ok(res)
}

pub async fn copy<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> io::Result<usize> {
    trace!("AVtrace123 copy: {:?} {:?}", from.as_ref(), to.as_ref());

    let from_file = get_file(from, false).await?;
    let from_handle = get_access_handle(&from_file).await?;
    let size = from_handle.get_size().unwrap() as usize;

    let to_file = get_file(to, true).await?;
    let to_handle = get_access_handle(&to_file).await?;

    let mut buf = Vec::with_capacity(size);
    let mut num_copied: usize = 0;
    let mut num_read: usize = 0;
    while num_read < size {
        num_read += from_handle
            .read_with_u8_array(&mut buf[num_read..])
            .map_err(js_val_to_io_error)? as usize;
        while num_copied < num_read {
            num_copied += to_handle
                .write_with_u8_array(&buf[num_copied..num_read])
                .map_err(js_val_to_io_error)? as usize;
        }
    }

    Ok(num_copied)
}

// TODO
// pub fn remove_dir_all<P: AsRef<Path>>(path: P) -> io::Result<()> {
//     Err(())
// }

pub async fn futurize<T: From<JsValue>>(promise: Promise) -> Result<T, Error> {
    // trace!("AVtrace123promise {:?}", &promise);
    let res = wasm_bindgen_futures::JsFuture::from(promise).await;
    if let Err(e) = &res {
        log!("GOT AN ERR ON PROMISE: {e:?}");
    }
    res.map_err(|_| Error::new(ErrorKind::Other, "none"))
        .map(|v| v.into())
}

pub fn get_global() -> DedicatedWorkerGlobalScope {
    JsValue::from(web_sys::js_sys::global()).into()
}

pub async fn get_root_directory() -> Result<FileSystemDirectoryHandle, Error> {
    let scope = get_global();
    let dir_promise = scope.navigator().storage().get_directory();
    futurize(dir_promise).await
}

pub(crate) fn js_val_to_io_error(v: JsValue) -> std::io::Error {
    std::io::Error::other(format!("{:?}", v.as_string()))
}

pub struct File {
    _handle: Mutex<FileSystemFileHandle>,
    access_handle: Mutex<FileSystemSyncAccessHandle>,
    pos: usize,
    path: PathBuf,
}

impl File {
    fn new<P: AsRef<Path>>(
        _handle: FileSystemFileHandle,
        access_handle: FileSystemSyncAccessHandle,
        path: P,
    ) -> Self {
        File {
            _handle: Mutex::new(_handle),
            access_handle: Mutex::new(access_handle),
            pos: 0,
            path: path.as_ref().to_path_buf(),
        }
    }

    pub async fn create<P: AsRef<Path>>(path: P) -> io::Result<File> {
        let file = get_file(&path, true).await?;
        let access_handle = get_access_handle(&file).await?;
        Ok(File::new(file, access_handle, &path))
    }

    pub async fn open<P: AsRef<Path>>(path: P) -> io::Result<File> {
        let file = get_file(&path, false).await?;
        let access_handle = get_access_handle(&file).await?;
        Ok(File::new(file, access_handle, &path))
    }

    pub async fn create_new<P: AsRef<Path>>(path: P) -> io::Result<File> {
        if get_file(path.as_ref(), false).await.is_ok() {
            log!("exists on create new: {:?}", path.as_ref());
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("already exists {:?}", path.as_ref()),
            ));
        }

        let file = get_file(&path, true).await?;
        let access_handle = get_access_handle(&file).await?;
        Ok(File::new(file, access_handle, &path))
    }

    pub(crate) fn truncate(&self) -> io::Result<()> {
        self.access_handle
            .lock()
            .unwrap()
            .truncate_with_u32(0)
            .map_err(js_val_to_io_error)
    }

    pub async fn persist<P: AsRef<Path>>(&mut self, to: P) -> io::Result<()> {
        log!("persist to {:?}", to.as_ref());
        self.access_handle.lock().unwrap().flush().unwrap();
        let size = self.access_handle.lock().unwrap().get_size().unwrap() as usize;
        let to_file = get_file(to, true).await?;
        let to_handle = get_access_handle(&to_file).await?;
        log!("size {size}");

        let mut buf = Vec::with_capacity(size);
        let mut num_read: usize = 0;
        let mut num_copied: usize = 0;
        while num_read < size {
            let read = self
                .access_handle
                .lock()
                .unwrap()
                .read_with_u8_array(&mut buf[num_read..])
                .unwrap();
            num_read += read as usize;
            while num_copied < num_read {
                num_copied += to_handle
                    .write_with_u8_array(&buf[num_copied..num_read])
                    .map_err(js_val_to_io_error)? as usize;
            }
        }
        to_handle.flush().unwrap();
        Ok(())
    }
}

impl Drop for File {
    fn drop(&mut self) {
        self.access_handle.lock().unwrap().close();
    }
}

impl io::Write for File {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        trace!("AVtrace123write at {}: {:?}", self.pos, self.path);
        let write_options = FileSystemReadWriteOptions::new();
        write_options.set_at(self.pos as f64);
        let num_written = self
            .access_handle
            .lock()
            .unwrap()
            .write_with_u8_array_and_options(buf, &write_options)
            .map_err(js_val_to_io_error)? as usize;
        self.pos += num_written;
        Ok(num_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        trace!("AVtrace123flush: {:?}", self.path);
        self.access_handle
            .lock()
            .unwrap()
            .flush()
            .map_err(js_val_to_io_error)
    }
}

impl io::Read for File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        trace!("AVtrace123read: {:?}", self.path);
        let read_options = FileSystemReadWriteOptions::new();
        read_options.set_at(self.pos as f64);
        let num_read = self
            .access_handle
            .lock()
            .unwrap()
            .read_with_u8_array_and_options(buf, &read_options)
            .map_err(js_val_to_io_error)? as usize;
        self.pos += num_read;
        Ok(num_read)
    }
}

impl io::Seek for File {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        trace!("AVtrace123seek: {:?}", self.path);
        if let SeekFrom::Start(p) = pos {
            self.pos = p as usize
        }
        Ok(self.pos as u64)
    }
}

#[derive(Debug, Default)]
pub struct OpenOptions {
    _read: bool,
    write: bool,
    append: bool,
    truncate: bool,
    create: bool,
    create_new: bool,
}

impl OpenOptions {
    pub fn new() -> Self {
        OpenOptions {
            _read: true,
            ..Default::default()
        }
    }

    pub fn read(&mut self, read: bool) -> &mut Self {
        self._read = read;
        self
    }

    pub fn write(&mut self, write: bool) -> &mut Self {
        self.write = write;
        self
    }

    pub fn append(&mut self, append: bool) -> &mut Self {
        self.append = append;
        self
    }

    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.truncate = truncate;
        self
    }

    pub fn create(&mut self, create: bool) -> &mut Self {
        self.create = create;
        self
    }

    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.create_new = create_new;
        self
    }

    pub async fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<File> {
        let OpenOptions {
            _read,
            write,
            append,
            truncate,
            create,
            create_new,
        } = self;
        if *create_new {
            if !*write || !*append {
                return Err(std::io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot create new without write or append permissions",
                ));
            }
            File::create_new(path.as_ref()).await?;
        } else if *create {
            if !*write || !*append {
                return Err(std::io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot create without write or append permissions",
                ));
            }
            File::create(path.as_ref()).await?;
        }
        let f = File::open(path).await?;
        if *truncate && !*create_new {
            f.truncate()?;
        }
        Ok(f)
    }
}

pub async fn remove_file<P: AsRef<Path>>(path: P) -> io::Result<()> {
    remove_entry(path, false).await
}

pub async fn remove_dir_all<P: AsRef<Path>>(path: P) -> io::Result<()> {
    remove_entry(path, true).await
}

async fn remove_entry<P: AsRef<Path>>(path: P, recursive: bool) -> io::Result<()> {
    let path = path.as_ref().normalize();
    let parent = if let Some(parent) = path.parent() {
        parent
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "attempting to remove root dir",
        ));
    };

    let name = path
        .file_name()
        .ok_or(io::Error::new(
            io::ErrorKind::InvalidInput,
            "could not get file name from path",
        ))?
        .to_str()
        .ok_or(io::Error::new(
            io::ErrorKind::InvalidInput,
            "could not get file name from path",
        ))?;

    let parent_dir = get_directory(parent, false).await?;
    // .map_err(js_val_to_io_error)?;
    let options = FileSystemRemoveOptions::new();
    options.set_recursive(recursive);

    let _: JsValue = futurize(parent_dir.remove_entry_with_options(name, &options)).await?;

    Ok(())
}
