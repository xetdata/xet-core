use std::path;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub fn new() -> TempDir {
        #[cfg(target_arch = "wasm32")]
        {
            let path = Path::new("tmp/hello").to_path_buf();
            let _ = dsfs::create_dir_all(path.clone());
            TempDir {
                path
            }
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            let path = tempfile::TempDir::new().unwrap();
            TempDir { path: path.into_path() }
        }
    }

    pub fn path(&self) -> &path::Path {
        self.path.as_ref()
    }
}
// #[derive(Debug, Clone, Eq, PartialEq)]
// pub struct Builder<'a, 'b> {
//     prefix: &'a OsStr,
//     suffix: &'b OsStr,
//     append: bool,
//     permissions: Option<std::fs::Permissions>,
//     keep: bool,
// }
// 
// impl<'a, 'b> Default for Builder<'a, 'b> {
//     fn default() -> Self {
//         Builder {
//             prefix: OsStr::new(".tmp"),
//             suffix: OsStr::new(""),
//             append: false,
//             permissions: None,
//             keep: false,
//         }
//     }
// }
// 
// impl<'a, 'b> Builder<'a, 'b> {
//     #[must_use]
//     pub fn new() -> Self {
//         Self::default()
//     }
// 
// 
//     pub fn prefix<S: AsRef<OsStr> + ?Sized>(&mut self, prefix: &'a S) -> &mut Self {
//         self.prefix = prefix.as_ref();
//         self
//     }
// 
//     pub fn suffix<S: AsRef<OsStr> + ?Sized>(&mut self, suffix: &'b S) -> &mut Self {
//         self.suffix = suffix.as_ref();
//         self
//     }
// 
//     pub fn append(&mut self, append: bool) -> &mut Self {
//         self.append = append;
//         self
//     }
// 
// 
//     pub fn permissions(&mut self, permissions: std::fs::Permissions) -> &mut Self {
//         self.permissions = Some(permissions);
//         self
//     }
// 
// 
//     pub fn keep(&mut self, keep: bool) -> &mut Self {
//         self.keep = keep;
//         self
//     }
// 
// 
//     pub fn tempfile_in<P: AsRef<Path>>(&self, dir: P) -> io::Result<NamedTempFile> {
//         create_helper(
//             dir.as_ref(),
//             self.prefix,
//             self.suffix,
//             |path| {
//                 file::create_named(
//                     path,
//                     OpenOptions::new().append(self.append),
//                     self.permissions.as_ref(),
//                     self.keep,
//                 )
//             },
//         )
//     }
// }
// 
// fn create_helper<R>(
//     base: &Path,
//     prefix: &OsStr,
//     suffix: &OsStr,
//     mut f: impl FnMut(PathBuf) -> io::Result<R>,
// ) -> io::Result<R> {
//     let num_retries = 1;
// 
//     for _ in 0..num_retries {
//         let path = base.join(tmpname(prefix, suffix, random_len));
//         return match f(path) {
//             Err(ref e) if e.kind() == io::ErrorKind::AlreadyExists && num_retries > 1 => continue,
//             // AddrInUse can happen if we're creating a UNIX domain socket and
//             // the path already exists.
//             Err(ref e) if e.kind() == io::ErrorKind::AddrInUse && num_retries > 1 => continue,
//             res => res,
//         };
//     }
// 
//     Err(io::Error::new(
//         io::ErrorKind::AlreadyExists,
//         "too many temporary files exist",
//     ))
//         .with_err_path(|| base)
// }
// 
// pub fn create_named(
//     mut path: PathBuf,
//     open_options: &mut OpenOptions,
//     permissions: Option<&std::fs::Permissions>,
//     keep: bool,
// ) -> io::Result<NamedTempFile> {
//     // Make the path absolute. Otherwise, changing directories could cause us to
//     // delete the wrong file.
//     if !path.is_absolute() {
//         path = std::env::current_dir()?.join(path)
//     }
//     imp::create_named(&path, open_options, permissions)
//         .with_err_path(|| path.clone())
//         .map(|file| NamedTempFile {
//             path: TempPath {
//                 path: path.into_boxed_path(),
//                 keep,
//             },
//             file,
//         })
// }
