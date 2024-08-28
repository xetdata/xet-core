#![feature(let_chains)]
#![cfg_attr(feature = "strict", deny(warnings))]

pub use async_iterator::*;
pub use buffered_async_iterator::*;
pub use parallel_utils::*;

mod parallel_utils;
mod async_iterator;
mod buffered_async_iterator;
