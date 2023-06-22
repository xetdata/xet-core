use gitxetcore::command::XetApp;
use gitxetcore::errors::MainReturn;

/// Note that in order for the exit code to work properly, then `MainReturn`,
/// not `errors::Result<()>`, should be returned. This is because Result<(), E>
/// already implements Terminate, which doesn't call E#report() if E implements
/// Terminate.
#[tokio::main]
async fn main() -> MainReturn {
    // Unfortunately, implementing Try operator (i.e. ?) for MainReturn is "unstable"...
    let app = match XetApp::init() {
        Ok(app) => app,
        Err(e) => return MainReturn::Error(e),
    };

    // TODO: would like to catch panics and fail gracefully, however, many structs (<=64) are not Send:
    // match app.run().catch_unwind().await {
    //     Ok(Ok(_)) => MainReturn::Success,
    //     Ok(Err(e)) => MainReturn::Error(e),
    //     Err(panic_err) => MainReturn::Panic(panic_err),
    // };
    match app.run().await {
        Ok(_) => MainReturn::Success,
        Err(e) => MainReturn::Error(e),
    }
}
