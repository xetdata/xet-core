#[cfg(target_arch = "wasm32")]
mod web_worker_fs;

#[cfg(target_arch = "wasm32")]
pub use web_worker_fs::*;

#[cfg(not(target_arch = "wasm32"))]
pub use std::fs::*;

#[cfg(unix)]
pub use std::os::unix::fs::*;

#[cfg(windows)]
pub use std::os::windows::fs::*;

#[macro_export]
macro_rules! dsfs_resolve {
    ($handle:expr) => {{
        #[cfg(target_arch = "wasm32")]
        {
            $handle.await
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            $handle
        }
    }};
}
