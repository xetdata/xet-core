use xet_error::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum KeyError {
    #[error("Key parsing failure: {0}")]
    UnparsableKey(String),
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SingleflightError<E>
where
    E: std::fmt::Debug,
{
    #[error("BUG: singleflight waiter was notified before result was updated")]
    NoResult,

    #[error("BUG: call was removed before singleflight owner could update it")]
    CallMissing,

    #[error("BUG: call didn't create a Notifier for the initial task")]
    NoNotifierCreated,

    #[error(transparent)]
    InternalError(#[from] E),

    #[error("Real call failed: {0}")]
    WaiterInternalError(String),

    #[error("JoinError inside singleflight owner task: {0}")]
    JoinError(String),

    #[error("Owner task panicked")]
    OwnerPanicked,
}

impl<E: std::fmt::Debug> Clone for SingleflightError<E> {
    fn clone(&self) -> Self {
        match self {
            SingleflightError::NoResult => SingleflightError::NoResult,
            SingleflightError::CallMissing => SingleflightError::CallMissing,
            SingleflightError::NoNotifierCreated => SingleflightError::NoNotifierCreated,
            SingleflightError::InternalError(e) => {
                SingleflightError::WaiterInternalError(format!("{e:?}"))
            }
            SingleflightError::WaiterInternalError(s) => {
                SingleflightError::WaiterInternalError(s.clone())
            }
            SingleflightError::JoinError(e) => SingleflightError::JoinError(e.clone()),
            SingleflightError::OwnerPanicked => SingleflightError::OwnerPanicked,
        }
    }
}
