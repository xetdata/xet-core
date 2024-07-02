use clap::Command;
use lazy_static::lazy_static;
use libxet::git_integration::run_git_captured;
use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};
use tokio::runtime::{Builder, Runtime};

thread_local! {
    static INTERPOSING_DISABLE_REQUESTS : AtomicU32 = AtomicU32::new(0);
}

// Guaranteed to be zero on library load for all the static initializers.
// This will only be initialized once we register a file pointer for our own use.
static FD_RUNTIME_INITIALIZED: AtomicBool = AtomicBool::new(false);

lazy_static! {
    pub static ref TOKIO_RUNTIME: Arc<Runtime> = {
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

        Arc::new(rt)
    };
}

pub fn activate_fd_runtime() {
    FD_RUNTIME_INITIALIZED.store(true, Ordering::SeqCst);
}

#[inline]
pub fn runtime_activated() -> bool {
    FD_RUNTIME_INITIALIZED.load(Ordering::Relaxed)
}

#[inline]
pub fn interposing_disabled() -> bool {
    if FD_RUNTIME_INITIALIZED.load(Ordering::Relaxed) {
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
        if errno::errno() != errno::Errno(0) {
            if FD_RUNTIME_INITIALIZED.load(Ordering::Relaxed) {
                // eprintln!("Errno: {:?}", errno::errno());
            }
        }
    }
}

pub fn with_interposing_disabled() -> InterposingDisable {
    INTERPOSING_DISABLE_REQUESTS.with(|v| v.fetch_add(1, Ordering::Relaxed));
    InterposingDisable {}
}

pub fn test_run_in_runtime() {
    if runtime_activated() {
        for _i in [0] {
            TOKIO_RUNTIME.handle().block_on(async move {
                let mut cmd = std::process::Command::new("git");
                cmd.arg("--version");

                cmd.env("LD_PRELOAD", "");

                // Set up the command to capture or pass through stdout and stderr
                // cmd.stdout(std::process::Stdio::piped());
                // cmd.stderr(std::process::Stdio::piped());
                cmd.stdout(std::process::Stdio::inherit());
                cmd.stderr(std::process::Stdio::piped());

                // Spawn the child
                let mut child = cmd.spawn().unwrap();

                // Immediately drop the writing end of the stdin pipe; if git attempts to wait on stdin, it will cause an error.
                drop(child.stdin.take());

                let out = child.wait_with_output().unwrap();

                eprintln!("SUBCOMMAND: output={out:?}");
            });

            eprintln!("MORE:");
        }

        //        TOKIO_RUNTIME.handle().block_on(async move {
        //            run_git_captured(None, "--version", &[], true, None).unwrap();
        //       });
    }
}
