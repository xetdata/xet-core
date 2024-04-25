#![cfg_attr(feature = "strict", allow(warnings))]

extern crate more_asserts;

pub mod environment;

pub mod command;
pub mod config;
pub mod constants;
pub mod data;
mod diff;
pub mod errors;
pub mod git_integration;
pub mod stream;
pub mod summaries;
mod utils;
pub mod xetblob;
pub mod xetmnt;
