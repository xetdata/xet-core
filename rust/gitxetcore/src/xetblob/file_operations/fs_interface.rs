use crate::errors::Result;
use crate::xetblob::DirEntry;

pub type FileInfo = DirEntry;

/// Trait to encapsulate the required FS operations for flexibility and abstraction.
#[async_trait::async_trait] // Use async_trait for async methods in traits
pub trait FileSystemOperations {
    async fn info(&self, path: &str) -> Result<FileInfo>;
    async fn find(&self, path: &str) -> Result<Vec<FileInfo>>;
    async fn glob(&self, pattern: &str) -> Result<Vec<FileInfo>>;
    fn is_dir(&self, path: &str) -> Result<bool>;
    fn is_xet(&self) -> bool; // Example method to check if the FS is Xet.
                              // Additional methods as needed...
}
