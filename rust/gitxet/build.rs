// build.rs to set up environment variables for running all this.
use std::env;

fn main() {
    let target_env = env::var("CARGO_CFG_TARGET_ENV").unwrap_or("".to_owned());

    if target_env == "msvc" {
        println!("cargo:rustc-link-arg=/stack:{}", 8 * 1024 * 1024);
    } else if target_env.is_empty() || target_env == "gnu" {
        println!(
            "cargo:rustc-link-arg=-Wl,-stack_size,{:#X}",
            8 * 1024 * 1024
        );
    }
}
