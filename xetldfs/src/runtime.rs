use crate::ENABLE_CALL_TRACING;
use lazy_static::lazy_static;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};
use tokio::runtime::{Builder, Runtime};

use crate::{ld_trace, ld_warn};

thread_local! {
    static INTERPOSING_DISABLE_REQUESTS : AtomicU32 = AtomicU32::new(0);
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
                INTERPOSING_DISABLE_REQUESTS.with(|init| {
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
    let result = tokio::task::block_in_place(|| TOKIO_RUNTIME.handle().block_on(future));

    // Step 4:
    if unsafe { libc::sigaction(SIGCHLD, &old_action, std::ptr::null_mut()) } != 0 {
        panic!("Failed to restore the previous SIGCHLD handler");
    }

    result
}

#[inline]
pub fn process_in_interposable_state() -> bool {
    let pid = unsafe { libc::getpid() as u64 };
    let s_pid = *FD_RUNTIME_PID;
    if pid == s_pid {
        true
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
    raw_runtime_activated() && process_in_interposable_state()
}

#[inline]
pub fn interposing_disabled() -> bool {
    if runtime_activated() {
        INTERPOSING_DISABLE_REQUESTS.with(|init| init.load(Ordering::Relaxed) != 0)
    } else {
        true
    }
}

pub struct InterposingDisable {}

impl Drop for InterposingDisable {
    fn drop(&mut self) {
        if runtime_activated() {
            let v = INTERPOSING_DISABLE_REQUESTS.with(|v| v.fetch_sub(1, Ordering::Relaxed));
            assert_ne!(v, 0);
        }
        // if errno::errno() != errno::Errno(0) {
        //    if FD_RUNTIME_INITIALIZED.load(Ordering::Relaxed) {
        //        // eprintln!("Errno: {:?}", errno::errno());
        //    }
        // }
    }
}

pub fn with_interposing_disabled() -> Option<InterposingDisable> {
    if runtime_activated() {
        INTERPOSING_DISABLE_REQUESTS.with(|v| v.fetch_add(1, Ordering::Relaxed));
        Some(InterposingDisable {})
    } else {
        None
    }
}
