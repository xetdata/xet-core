use std::path::PathBuf;

pub fn expand_tilde(path: &str) -> PathBuf {
    if path.starts_with("~") {
        if let Some(home_dir) = dirs::home_dir() {
            return home_dir.join(path.trim_start_matches("~/"));
        }
    }
    PathBuf::from(path)
}
