use std::fmt::{Debug, Display};

#[cfg(not(target_family = "wasm"))]
use tracing::{debug, error, info, warn};

#[cfg(target_family = "wasm")]
macro_rules! log {
    ( $( $t:tt )* ) => {
        web_sys::console::log_1(&format!( $( $t )* ).into());
    }
}

/// A helper trait to log errors.
/// The logging functions will track the caller's callsite.
/// For a chain of calls A -> B -> C -> ErrorPrinter, the
/// topmost function without #[track_caller] is deemed the callsite.
pub trait ErrorPrinter {
    fn log_error<M: Display>(self, message: M) -> Self;

    fn warn_error<M: Display>(self, message: M) -> Self;

    fn debug_error<M: Display>(self, message: M) -> Self;

    fn info_error<M: Display>(self, message: M) -> Self;
}

impl<T, E: Debug> ErrorPrinter for Result<T, E> {
    /// If self is an Err(e), prints out the given string to tracing::error,
    /// appending "error: {e}" to the end of the message.
    #[track_caller]
    fn log_error<M: Display>(self, message: M) -> Self {
        if let Err(e) = &self {
            #[cfg(not(target_family = "wasm"))]
            {
                let location = std::panic::Location::caller();
                error!(
                    caller = format!("{}:{}", location.file(), location.line()),
                    "{}, error: {:?}", message, e
                )
            }
            #[cfg(target_family = "wasm")]
            log!("{}, error: {:?}", message, e);
        }
        self
    }

    /// If self is an Err(e), prints out the given string to tracing::warn,
    /// appending "error: {e}" to the end of the message.
    #[track_caller]
    fn warn_error<M: Display>(self, message: M) -> Self {
        if let Err(e) = &self {
            #[cfg(not(target_family = "wasm"))]
            {
                let location = std::panic::Location::caller();
                warn!(
                    caller = format!("{}:{}", location.file(), location.line()),
                    "{}, error: {:?}", message, e
                )
            }
            #[cfg(target_family = "wasm")]
            log!("{}, warn: {:?}", message, e);
        }
        self
    }

    /// If self is an Err(e), prints out the given string to tracing::debug,
    /// appending "error: {e}" to the end of the message.
    #[track_caller]
    fn debug_error<M: Display>(self, message: M) -> Self {
        if let Err(e) = &self {
            #[cfg(not(target_family = "wasm"))]
            {
                let location = std::panic::Location::caller();
                debug!(
                    caller = format!("{}:{}", location.file(), location.line()),
                    "{}, error: {:?}", message, e
                )
            }
            #[cfg(target_family = "wasm")]
            log!("{}, debug: {:?}", message, e);
        }
        self
    }

    /// If self is an Err(e), prints out the given string to tracing::info,
    /// appending "error: {e}" to the end of the message.
    #[track_caller]
    fn info_error<M: Display>(self, message: M) -> Self {
        if let Err(e) = &self {
            #[cfg(not(target_family = "wasm"))]
            {
                let location = std::panic::Location::caller();
                info!(
                    caller = format!("{}:{}", location.file(), location.line()),
                    "{}, error: {:?}", message, e
                )
            }
            #[cfg(target_family = "wasm")]
            log!("{}, info: {:?}", message, e);
        }
        self
    }
}
