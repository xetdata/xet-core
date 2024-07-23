use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::fs::{self, File, Metadata};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::{create_file, file_metadata::set_file_metadata};

pub struct SafeFileCreator {
    dest_path: PathBuf,
    temp_path: PathBuf,
    original_metadata: Option<Metadata>,
    writer: Option<BufWriter<File>>,
}

impl SafeFileCreator {
    /// Safely creates a new file at a specific location.  Ensures the file is not created with elevated privileges,
    /// and a temporary file is created then renamed on close.
    pub fn new<P: AsRef<Path>>(dest_path: P) -> io::Result<Self> {
        let dest_path = dest_path.as_ref().to_path_buf();
        let temp_path = Self::temp_file_path(&dest_path);

        // This matches the permissions and ownership of the parent directory
        let file = create_file(&temp_path)?;
        let writer = BufWriter::new(file);

        Ok(SafeFileCreator {
            dest_path,
            temp_path,
            original_metadata: None,
            writer: Some(writer),
        })
    }

    /// Safely replaces a new file at a specific location.  Ensures the file is not created with elevated privileges,
    /// and additionally the metadata of the old one will match the new metadata.
    pub fn replace_existing<P: AsRef<Path>>(dest_path: P) -> io::Result<Self> {
        let mut s = Self::new(&dest_path)?;
        s.original_metadata = std::fs::metadata(dest_path).ok();
        Ok(s)
    }

    /// Generates a temporary file path in the same directory as the destination file
    pub fn temp_file_path<P: AsRef<Path>>(dest_path: P) -> PathBuf {
        let path = dest_path.as_ref();
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let file_name = path.file_name().unwrap().to_str().unwrap();

        let mut rng = thread_rng();
        let random_hash: String = (0..10)
            .map(|_| rng.sample(Alphanumeric))
            .map(char::from)
            .collect();
        let temp_file_name = format!(".{}.{hash}.tmp", file_name, hash = random_hash);
        parent.join(temp_file_name)
    }

    /// Closes the writer and replaces the original file with the temporary file
    pub fn close(&mut self) -> io::Result<()> {
        let Some(mut writer) = self.writer.take() else {
            return Ok(());
        };

        writer.flush()?;
        drop(writer);

        // Replace the original file with the new file
        fs::rename(&self.temp_path, &self.dest_path)?;

        if let Some(metadata) = self.original_metadata.as_ref() {
            set_file_metadata(&self.dest_path, metadata, false)?;
        }

        Ok(())
    }

    fn writer(&mut self) -> io::Result<&mut BufWriter<File>> {
        match &mut self.writer {
            Some(wr) => Ok(wr),
            None => Err(std::io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("Writing to {:?} already completed.", &self.dest_path),
            )),
        }
    }
}

impl Write for SafeFileCreator {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer()?.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer()?.flush()
    }
}

impl Drop for SafeFileCreator {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            eprintln!(
                "Error: Failed to close writer for {:?}: {}",
                &self.dest_path, e
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::Read;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::tempdir;

    #[test]
    fn test_safe_file_creator_new() {
        let dir = tempdir().unwrap();
        let dest_path = dir.path().join("new_file.txt");

        let mut safe_file_creator = SafeFileCreator::new(&dest_path).unwrap();
        writeln!(safe_file_creator, "Hello, world!").unwrap();
        safe_file_creator.close().unwrap();

        // Verify file contents
        let mut contents = String::new();
        File::open(&dest_path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        assert_eq!(contents.trim(), "Hello, world!");

        // Verify file permissions
        let metadata = fs::metadata(&dest_path).unwrap();
        let permissions = metadata.permissions();
        assert_eq!(permissions.mode() & 0o777, 0o644); // Assuming default creation mode
    }

    #[test]
    fn test_safe_file_creator_replace_existing() {
        let dir = tempdir().unwrap();
        let dest_path = dir.path().join("existing_file.txt");

        // Create the existing file
        {
            let mut file = File::create(&dest_path).unwrap();
            file.write_all(b"Old content").unwrap();
            let mut perms = file.metadata().unwrap().permissions();
            perms.set_mode(0o600);
            fs::set_permissions(&dest_path, perms).unwrap();
        }

        let mut safe_file_creator = SafeFileCreator::replace_existing(&dest_path).unwrap();
        writeln!(safe_file_creator, "New content").unwrap();
        safe_file_creator.close().unwrap();

        // Verify file contents
        let mut contents = String::new();
        File::open(&dest_path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        assert_eq!(contents.trim(), "New content");

        // Verify file permissions
        let metadata = fs::metadata(&dest_path).unwrap();
        let permissions = metadata.permissions();
        assert_eq!(permissions.mode() & 0o777, 0o600); // Original file mode
    }

    #[test]
    fn test_safe_file_creator_drop() {
        let dir = tempdir().unwrap();
        let dest_path = dir.path().join("drop_file.txt");

        {
            let mut safe_file_creator = SafeFileCreator::new(&dest_path).unwrap();
            writeln!(safe_file_creator, "Hello, world!").unwrap();
            // safe_file_creator is dropped here
        }

        // Verify file contents
        let mut contents = String::new();
        File::open(&dest_path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        assert_eq!(contents.trim(), "Hello, world!");
    }

    #[test]
    fn test_safe_file_creator_double_close() {
        let dir = tempdir().unwrap();
        let dest_path = dir.path().join("double_close_file.txt");

        let mut safe_file_creator = SafeFileCreator::new(&dest_path).unwrap();
        writeln!(safe_file_creator, "Hello, world!").unwrap();
        safe_file_creator.close().unwrap();
        safe_file_creator.close().unwrap(); // Should be a no-op

        // Verify file contents
        let mut contents = String::new();
        File::open(&dest_path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        assert_eq!(contents.trim(), "Hello, world!");
    }

    #[test]
    #[cfg(unix)]
    fn test_safe_file_creator_set_metadata() {
        let dir = tempdir().unwrap();
        let dest_path = dir.path().join("metadata_file.txt");

        // Create the existing file
        {
            let mut file = File::create(&dest_path).unwrap();
            file.write_all(b"Old content").unwrap();
            let mut perms = file.metadata().unwrap().permissions();
            perms.set_mode(0o600);
            fs::set_permissions(&dest_path, perms).unwrap();
        }

        let mut safe_file_creator = SafeFileCreator::replace_existing(&dest_path).unwrap();
        writeln!(safe_file_creator, "New content").unwrap();
        safe_file_creator.close().unwrap();

        // Verify file contents
        let mut contents = String::new();
        File::open(&dest_path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        assert_eq!(contents.trim(), "New content");

        // Verify file permissions
        let metadata = fs::metadata(&dest_path).unwrap();
        let permissions = metadata.permissions();
        assert_eq!(permissions.mode() & 0o777, 0o600); // Original file mode
    }
}
