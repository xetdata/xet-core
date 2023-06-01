// build.rs
use std::env;
use std::process::ExitCode;

fn main() -> Result<(), ExitCode> {
    if env::var("CARGO_CFG_TARGET_OS").unwrap() == "windows" {
        // On Windows, don't attempt to build libmagic. It will fail.
        return Ok(());
    }

    cc::Build::new()
        .file("bzlib.c")
        .file("crctable.c")
        .file("decompress.c")
        .file("huffman.c")
        .file("randtable.c")
        .compile("bzip2");

    // if the contents of the current source directory has changed,
    // we want to re-run this file. (preserving default behavior)
    println!("cargo:rerun-if-changed=./");
    Ok(())
}
