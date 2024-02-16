// build.rs to set up environment variables for running all this.
use std::env;

fn main() {
    if env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc") {
        println!("cargo:rustc-link-arg=/stack:{}", 2 * 1024 * 1024);
    } else if cfg!(target_os = "windows") {
        println!("cargo:rustc-link-arg=-Wl,--stack,{}", 2 * 1024 * 1024);
    }
}
