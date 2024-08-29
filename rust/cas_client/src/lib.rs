#![cfg_attr(feature = "strict", deny(warnings))]

pub use crate::error::CasClientError;
pub use interface::Client;
pub use local_client::LocalClient;
pub use merklehash::MerkleHash; // re-export since this is required for the client API.
pub use remote_client::set_trace_forwarding;
pub use remote_client::trace_forwarding;
pub use remote_client::DSCASAPIClient;
pub use remote_client::RemoteClient;
pub use remote_client::CAS_PROTOCOL_VERSION;

mod client_adapter;
mod error;
mod interface;
mod local_client;
mod remote_client;
