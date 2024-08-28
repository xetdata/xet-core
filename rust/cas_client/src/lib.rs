#![cfg_attr(feature = "strict", deny(warnings))]

pub use caching_client::CachingClient;
// pub use grpc::set_trace_forwarding;
// pub use grpc::GrpcClient;
pub use interface::Client;
pub use local_client::LocalClient;
pub use merklehash::MerkleHash;
// re-export since this is required for the client API.
pub use passthrough_staging_client::PassthroughStagingClient;
// pub use remote_client::RemoteClient;
// pub use remote_client::CAS_PROTOCOL_VERSION;
pub use staging_client::{new_staging_client, new_staging_client_with_progressbar, StagingClient};
pub use staging_trait::{Staging, StagingBypassable};

pub use crate::error::CasClientError;

mod caching_client;
mod cas_connection_pool;
mod client_adapter;
mod data_transport;
mod error;
pub mod grpc;
mod interface;
mod local_client;
mod passthrough_staging_client;
mod remote_client;
mod staging_client;
mod staging_trait;
mod util;
