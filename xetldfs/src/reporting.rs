pub const ENABLE_CALL_TRACING: bool = false;
pub const ENABLE_CALL_TRACING_FULL: bool = false;

#[macro_export]
macro_rules! ld_trace {
    ($($arg:tt)*) => {{
        use crate::reporting::ENABLE_CALL_TRACING;
        if ENABLE_CALL_TRACING {
            if $crate::runtime::raw_runtime_activated() {
                let text = format!("XetLDFS[{}, {}:{}]: {}", unsafe {libc::getpid() }, file!(), line!(), format!($($arg)*));
                eprintln!("{text}");
            }
        }
    }};
}

#[macro_export]
macro_rules! ld_func_trace {
    ($func_name:expr, $($var:ident),*) => {{
        use crate::reporting::ENABLE_CALL_TRACING_FULL;
        if ENABLE_CALL_TRACING_FULL {
            if $crate::runtime::raw_runtime_activated() {
                let mut out = String::new();
                $(
                    out.push_str(&format!("{}={:?} ", stringify!($var), $var));
                )*
                $crate::ld_trace!("{out}");
            }
        }
    }};
}

#[macro_export]
macro_rules! ld_warn {
    ($($arg:tt)*) => {
        let text = {
            if cfg!(debug_assertions) {
                format!("XetLDFS WARNING ([{}] {}:{}): {}", unsafe {libc::getpid() }, file!(), line!(), format!($($arg)*))
            } else {
                format!("XetLDFS WARNING: {}", format!($($arg)*))
            }
        };

        eprintln!("{text}");

        if $crate::runtime::XET_LOGGING_INITIALIZED.load(std::sync::atomic::Ordering::Relaxed) {
            use tracing::warn;
            $crate::runtime::tokio_run(async move { warn!("{text}") });
        }
    };
}

#[macro_export]
macro_rules! ld_error {
    ($($arg:tt)*) => {
        let text = {
            if cfg!(debug_assertions) {
                format!("XetLDFS ERROR ([{}] {}:{}): {}", unsafe {libc::getpid() }, file!(), line!(), format!($($arg)*))
            } else {
                format!("XetLDFS ERROR: {}", format!($($arg)*))
            }
        };

        eprintln!("{text}");

        if $crate::runtime::XET_LOGGING_INITIALIZED.load(std::sync::atomic::Ordering::Relaxed) {
            use tracing::error;
            $crate::runtime::tokio_run(async move { error!("{text}") });
        }
    };
}

#[macro_export]
macro_rules! ld_io_error {
    ($($arg:tt)*) => {{
        use errno::errno;
        crate::ld_error!("{} ({:?})", format!($($arg)*), errno());
    }};
}
