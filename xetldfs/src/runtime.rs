use lazy_static::lazy_static;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};
use tokio::runtime::{Builder, Runtime};

thread_local! {
    static INTERPOSING_DISABLE_REQUESTS : AtomicU32 = AtomicU32::new(0);
}

// Guaranteed to be zero on library load for all the static initializers.
// This will only be initialized once we register a file pointer for our own use.
static FD_RUNTIME_INITIALIZED: AtomicI64 = AtomicI64::new(0);

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

    tokio::task::block_in_place(|| TOKIO_RUNTIME.handle().block_on(future))
}

pub fn activate_fd_runtime() {
    let pid = unsafe { libc::getpid() } as i64;
    let s_pid = FD_RUNTIME_INITIALIZED.swap(pid, Ordering::SeqCst);
    assert!(pid == 0 || s_pid == pid);
}

#[inline]
pub fn runtime_activated() -> bool {
    let s_pid = FD_RUNTIME_INITIALIZED.load(Ordering::Relaxed);
    (s_pid != 0) && (unsafe { libc::getpid() } as i64) == s_pid
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
        let v = INTERPOSING_DISABLE_REQUESTS.with(|v| v.fetch_sub(1, Ordering::Relaxed));
        assert_ne!(v, 0);
        // if errno::errno() != errno::Errno(0) {
        //    if FD_RUNTIME_INITIALIZED.load(Ordering::Relaxed) {
        //        // eprintln!("Errno: {:?}", errno::errno());
        //    }
        // }
    }
}

pub fn with_interposing_disabled() -> InterposingDisable {
    INTERPOSING_DISABLE_REQUESTS.with(|v| v.fetch_add(1, Ordering::Relaxed));
    InterposingDisable {}
}
