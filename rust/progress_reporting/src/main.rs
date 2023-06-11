use core::time;
use progress_reporting::DataProgressReporter;
use std::thread::sleep;

fn main() {
    let mut pb = DataProgressReporter::new("Testing progress bar", Some(10));

    pb.register_progress(Some(1), 25 * 1024);
    sleep(time::Duration::from_millis(500));
    pb.register_progress(Some(8), 50 * 1024);
    sleep(time::Duration::from_millis(500));

    pb.finalize();

    let mut pb = DataProgressReporter::new("Testing progress bar, bytes only", None);

    pb.register_progress(None, 25 * 1024);
    sleep(time::Duration::from_millis(500));
    pb.register_progress(None, 50 * 1024);
    sleep(time::Duration::from_millis(500));

    pb.finalize();
}
