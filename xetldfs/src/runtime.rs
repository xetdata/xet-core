use lazy_static::lazy_static;
use libxet::config::XetConfig;
use std::{
    fmt::{Debug, Display},
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        RwLock,
    },
};
use tokio::runtime::{Builder, Runtime};

use crate::ld_trace;

thread_local! {
    // External interposing is a bit less strict, as interactions between interposed
    // functions (e.g. stat) sometimes need to be interposed to be correct.
    static EXTERNAL_INTERPOSING_GUARDS : AtomicU32 = const { AtomicU32::new(0) };

    // For internal processes, we need the interposing to be complete and strict;
    // this covers all requests within xet-core.
    // Here, no functions (stat functions included) should be interposed.
    static ABSOLUTE_INTERPOSING_DISABLE_GUARDS: AtomicU32 = const { AtomicU32::new(0) };
}

// Guaranteed to be zero on library load for all the static initializers.
// This will only be initialized once we register a file pointer for our own use.
lazy_static! {
    static ref FD_RUNTIME_PID: u64 = unsafe { libc::getpid() as u64 };
}

static FD_RUNTIME_INITIALIZED: AtomicBool = AtomicBool::new(false);

lazy_static! {
    static ref TOKIO_RUNTIME: Runtime = {
        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .on_thread_start(|| {
                ABSOLUTE_INTERPOSING_DISABLE_GUARDS.with(|init| {
                    init.store(1, Ordering::Relaxed);
                });
            })
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");

        rt
    };
}

pub fn tokio_run<F: std::future::Future>(future: F) -> F::Output {
    // This should never happen; is a problem.
    assert!(runtime_activated());

    use libc::{sigaction, sighandler_t, SIGCHLD, SIG_DFL};

    // Save the current SIGCHLD signal handler so that bash doesn't interfere with the
    // child processes.
    let mut old_action: sigaction = unsafe { std::mem::zeroed() };
    if unsafe { libc::sigaction(SIGCHLD, std::ptr::null(), &mut old_action) } != 0 {
        panic!("Failed to get the current SIGCHLD handler");
    }

    // Step 2: Set SIGCHLD to SIG_DFL (default action)
    let mut new_action: sigaction = unsafe { std::mem::zeroed() };
    new_action.sa_sigaction = SIG_DFL as sighandler_t;
    new_action.sa_flags = 0;
    if unsafe { libc::sigaction(SIGCHLD, &new_action, std::ptr::null_mut()) } != 0 {
        panic!("Failed to set SIGCHLD to default handler");
    }

    // Step 3: Run all the tokio stuff, which may include spawning other processes.
    let result = tokio::task::block_in_place(|| {
        // This is needed as the task::block_in_place may either run this on the current thread or
        // spin up a new thread to prevent tokio threads from blocking,
        // so this ensures that interposing is disabled within this.
        let _lg = absolute_interposing_disable();

        TOKIO_RUNTIME.handle().block_on(async move {
            // May not be necessary here but doesn't hurt.
            let _lg = absolute_interposing_disable();
            future.await
        })
    });

    // Step 4:
    if unsafe { libc::sigaction(SIGCHLD, &old_action, std::ptr::null_mut()) } != 0 {
        panic!("Failed to restore the previous SIGCHLD handler");
    }

    result
}

/// Convenience method to run a tokio future and correct the
pub fn tokio_run_error_checked<T, E, F: std::future::Future<Output = std::result::Result<T, E>>>(
    context: &str,
    future: F,
) -> Option<T>
where
    T: Send + Sync,
    E: Display + Debug + Sync + Send,
{
    tokio_run(future)
        .map_err(|e| {
            if cfg!(debug_assertions) {
                ld_error!("Error in {context}: {e:?}");
            } else {
                ld_error!("Error in {context}: {e}");
            }
            e
        })
        .ok()
}

#[inline]
pub fn interposing_possible() -> bool {
    let pid = unsafe { libc::getpid() as u64 };
    let s_pid = *FD_RUNTIME_PID;
    if pid == s_pid {
        let ok = ABSOLUTE_INTERPOSING_DISABLE_GUARDS.with(|init| init.load(Ordering::Relaxed) == 0);
        if !ok {
            ld_trace!("XetLDFS: Disabling interposing from internal interposing state.");
        }
        ok
    } else {
        ld_trace!("XetLDFS: process not in interposable state: {pid} != {s_pid}");
        false
    }
}

pub fn activate_fd_runtime() {
    FD_RUNTIME_INITIALIZED.store(true, Ordering::SeqCst);
}

#[inline]
pub fn raw_runtime_activated() -> bool {
    FD_RUNTIME_INITIALIZED.load(Ordering::Relaxed)
}

#[inline]
pub fn runtime_activated() -> bool {
    raw_runtime_activated() && interposing_possible()
}

#[inline]
pub fn interposing_disabled() -> bool {
    if runtime_activated() {
        EXTERNAL_INTERPOSING_GUARDS.with(|init| init.load(Ordering::Relaxed) != 0)
    } else {
        true
    }
}

pub struct InterposingDisable {}

impl Drop for InterposingDisable {
    fn drop(&mut self) {
        let v = EXTERNAL_INTERPOSING_GUARDS.with(|v| v.fetch_sub(1, Ordering::Relaxed));
        assert_ne!(v, 0);
    }
}

pub fn with_interposing_disabled() -> InterposingDisable {
    EXTERNAL_INTERPOSING_GUARDS.with(|v| v.fetch_add(1, Ordering::Relaxed));
    InterposingDisable {}
}

////////////////////////
// Internal
pub struct AbsoluteInterposingDisable {}

impl Drop for AbsoluteInterposingDisable {
    fn drop(&mut self) {
        let v = ABSOLUTE_INTERPOSING_DISABLE_GUARDS.with(|v| v.fetch_sub(1, Ordering::Relaxed));
        assert_ne!(v, 0);
    }
}

pub fn absolute_interposing_disable() -> AbsoluteInterposingDisable {
    ABSOLUTE_INTERPOSING_DISABLE_GUARDS.with(|v| v.fetch_add(1, Ordering::Relaxed));
    AbsoluteInterposingDisable {}
}

/////////////
// Some things for the Xet Runtime

pub static XET_RUNTIME_INITIALIZED: AtomicBool = AtomicBool::new(false);
pub static XET_LOGGING_INITIALIZED: AtomicBool = AtomicBool::new(false);
pub static XET_ENVIRONMENT_CFG: RwLock<Option<XetConfig>> = RwLock::new(None);

/// This function initializes the xet runtime, setting up logging and ssl cert stuff.
pub fn initialize_xet_runtime() {
    ld_trace!("Initializing Runtime.");

    if XET_RUNTIME_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }

    let mut env_cfg = XET_ENVIRONMENT_CFG.write().unwrap();

    if env_cfg.is_none() {
        let _lg = with_interposing_disabled();

        let Ok(cfg) = XetConfig::new(None, None, libxet::config::ConfigGitPathOption::NoPath)
            .map_err(|e| {
                ld_error!("Error initializing Xet Config, Runtime Disabled: {e}");
                e
            })
        else {
            return;
        };

        match libxet::environment::log::initialize_tracing_subscriber(&cfg) {
            Err(e) => {
                ld_error!("Error initializing logging tracing subscriber: {e}");
            }
            Ok(_) => {
                XET_LOGGING_INITIALIZED.store(true, Ordering::SeqCst);
            }
        };

        // Probe to find the ssl cert dirs.
        let _ = openssl_probe::try_init_ssl_cert_env_vars();

        *env_cfg = Some(cfg);
    }

    XET_RUNTIME_INITIALIZED.store(true, Ordering::SeqCst);
}

/// Returns the xet base config, ensuring things are initialized.
pub fn xet_cfg() -> Option<XetConfig> {
    if !XET_RUNTIME_INITIALIZED.load(Ordering::Relaxed) {
        initialize_xet_runtime();
    }

    XET_ENVIRONMENT_CFG
        .read()
        .unwrap()
        .as_ref()
        .map(|v| v.clone())
}
