#![cfg_attr(feature = "strict", deny(warnings))]

mod cfg;
mod console_ser;
mod error;
mod level;
mod loader;

pub use cfg::{Axe, Cache, Cas, Cfg, Log, User, Xs3};
pub use cfg::{
    DEFAULT_CACHE_PATH_UNDER_HOME, DEFAULT_CAS_PREFIX, DEFAULT_XET_HOME, PROD_AXE_CODE,
    PROD_CAS_ENDPOINT, XET_CAS_SERVER_ENV_VAR,
};
pub use error::CfgError;
pub use level::Level;
pub use loader::XetConfigLoader;
