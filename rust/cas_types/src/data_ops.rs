use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PostResponse {
    pub was_inserted: bool,
}
