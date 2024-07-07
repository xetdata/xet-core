use clap::Command;
use lazy_static::lazy_static;
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Mutex as TMutex;

use crate::xet_rfile::XetFdReadHandle;

// The first place to go. 
// Guaranteed to be zero on library load for all the static initializers.
// This will only be initialized once we register a file pointer for our own use.
static FD_RUNTIME_INITIALIZED: AtomicBool = AtomicBool::new(false);

thread_local! {
    static INTERPOSING_DISABLE_REQUESTS : AtomicU32 = AtomicU32::new(0);
}

pub struct ProcessRuntime {
    runtime: RwLock<Option<Arc<Runtime>>>,
    registered_fds: RwLock<HashMap<c_int, XetFdReadHandle>>,
    xet_repo_wrappers: Vec<Arc<XetFSRepoWrapper>>,
    xet_environment_cfg: TMutex<Option<XetConfig>>,
}

// Thread local storage is important in this case, as it is not propegated 
// as a shared memory state through the clone3 call that bash uses to spin 
// up a child process.  So with this, what we use is actually a 
thread_local! {
    static THREAD_RUNTIME: Cell<Option<Arc<ProcessRuntime>>> = Cell::new(None);
}

static PID_RT_MAP : RwLock<BTreeMap<u32, Arc<ProcessRuntime> > > = RwLock::new(BTreeMap::new());

pub fn get_runtime() -> Arc<ProcessRuntime> {
    THREAD_RUNTIME.with_borrow_mut(|v| {
        if let Some(rt) = v {
            return rt.clone();
        }

        // Now we need to see if there's an entry in the process id table. 
        let pid = std::process::id();

        let rt = PID_RT_MAP.read().unwrap().get(&pid);

        if let Some(rtv) = rt {
            *v = rt.clone(); 
            return rt.clone(); 
        }

        PID_RT_MAP.write().unwrap().entry(&pid).or_insert_with(|| ProcessRuntime::default()).clone()
    })
}



impl ProcessRuntime {

    pub fn tokio(&self) -> Arc<Runtime> {
        if let Some(trt) = self.runtime.read().unwrap() {
            return trt.clone();
        }

        let rt_lg = self.runtime.write().unwrap();
        if let Some(trt) = rt_lg {
            return trt.clone();
        }

        let trt = Arc::new(Builder::new_multi_thread()
            .worker_threads(1)
            .on_thread_start(|| {
                INTERPOSING_DISABLE_REQUESTS.with(|init| {
                    init.store(1, Ordering::Relaxed);
                });
            })
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime"));

        *rt_lg = trt.clone();
        trt
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
            // TOKIO_RUNTIME.handle().block_on(async move {
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

                let out = child.wait_with_output().await.unwrap();

                eprintln!("SUBCOMMAND: output={out:?}");
            //});

            eprintln!("MORE:");

        //        TOKIO_RUNTIME.handle().block_on(async move {
        //            run_git_captured(None, "--version", &[], true, None).unwrap();
        //       });
    }
}
