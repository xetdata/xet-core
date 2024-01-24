use std::fmt::{Debug, Display};
use tracing::{debug, error, info, warn};

pub trait ErrorPrinter {
    fn log_error<M: Display>(self, message: M) -> Self;

    fn warn_error<M: Display>(self, message: M) -> Self;

    fn debug_error<M: Display>(self, message: M) -> Self;

    fn info_error<M: Display>(self, message: M) -> Self;
}

impl<T, E: Debug> ErrorPrinter for Result<T, E> {
    /// If self is an Err(e), prints out the given string to tracing::error,
    /// appending "error: {e}" to the end of the message.
    fn log_error<M: Display>(self, message: M) -> Self {
        match &self {
            Ok(_) => {}
            Err(e) => error!("{}, error: {:?}", message, e),
        }
        self
    }

    /// If self is an Err(e), prints out the given string to tracing::warn,
    /// appending "error: {e}" to the end of the message.
    fn warn_error<M: Display>(self, message: M) -> Self {
        match &self {
            Ok(_) => {}
            Err(e) => warn!("{}, error: {:?}", message, e),
        }
        self
    }

    /// If self is an Err(e), prints out the given string to tracing::debug,
    /// appending "error: {e}" to the end of the message.
    fn debug_error<M: Display>(self, message: M) -> Self {
        match &self {
            Ok(_) => {}
            Err(e) => debug!("{}, error: {:?}", message, e),
        }
        self
    }

    /// If self is an Err(e), prints out the given string to tracing::info,
    /// appending "error: {e}" to the end of the message.
    fn info_error<M: Display>(self, message: M) -> Self {
        match &self {
            Ok(_) => {}
            Err(e) => info!("{}, error: {:?}", message, e),
        }
        self
    }
}
