use lazy_static::lazy_static;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::runtime::{Builder, Runtime};

thread_local! {
    static IN_LOCAL_RUNTIME_THREAD : AtomicBool = AtomicBool::new(false);
}

lazy_static! {
    pub static ref TOKIO_RUNTIME: Arc<Runtime> = {
        let rt = Builder::new_multi_thread()
            .worker_threads(4)
            .on_thread_start(|| {
                IN_LOCAL_RUNTIME_THREAD.with(|init| {
                    init.store(true, Ordering::SeqCst);
                });
            })
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");

        Arc::new(rt)
    };
}

pub fn in_local_runtime() -> bool {
    IN_LOCAL_RUNTIME_THREAD.with(|init| init.load(Ordering::SeqCst))
}
