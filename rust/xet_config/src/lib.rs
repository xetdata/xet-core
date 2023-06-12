#![cfg_attr(feature = "strict", deny(warnings))]

mod cfg;
mod console_ser;
mod error;
mod level;
mod loader;

pub use cfg::DEFAULT_CAS_PREFIX;
pub use cfg::{Axe, Cache, Cas, Cfg, Log, User};
pub use error::CfgError;
pub use level::Level;
pub use loader::XetConfigLoader;
