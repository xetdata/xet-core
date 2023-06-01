// build.rs
use std::env;
use std::process::ExitCode;

fn main() -> Result<(), ExitCode> {
    if env::var("CARGO_CFG_TARGET_OS").unwrap() == "windows" {
        // On Windows, don't attempt to build libmagic. It will fail.
        return Ok(());
    }

    let mut cfg = cc::Build::new();
    cfg.file("src/apprentice.c");
    cfg.file("src/ascmagic.c");
    cfg.file("src/buffer.c");
    cfg.file("src/bzip2_build_hack.c");
    cfg.file("src/cdf.c");
    cfg.file("src/cdf_time.c");
    cfg.file("src/compress.c");
    cfg.file("src/der.c");
    cfg.file("src/encoding.c");
    cfg.file("src/fmtcheck.c");
    cfg.file("src/funcs.c");
    cfg.file("src/is_csv.c");
    cfg.file("src/is_json.c");
    cfg.file("src/is_tar.c");
    cfg.file("src/magic.c");
    cfg.file("src/print.c");
    cfg.file("src/readcdf.c");
    cfg.file("src/softmagic.c");

    #[cfg(target_os = "linux")]
    {
        cfg.file("src/strlcpy.c");
    }

    cfg.flag("-includestdint.h");
    cfg.flag("-includeinttypes.h");
    cfg.flag("-includeunistd.h");
    cfg.include("../bzip2/bzlib.h");
    cfg.flag("-DHAVE_SYS_WAIT_H=1");
    cfg.flag("-DHAVE_SYS_TIME_H=1");
    cfg.flag("-DZLIBSUPPORT=1");
    cfg.flag("-DHAVE_ZLIB_H=1");
    //cfg.flag("-DBZLIBSUPPORT=1");
    //cfg.flag("-DHAVE_BZLIB_H=1");
    cfg.flag("-DHAVE_FORK=1");
    cfg.flag("-w"); // suppress warnings
    cfg.compile("magic");

    //println!("cargo:rustc-link-lib=static=bzip2");

    // if the contents of the current source directory has changed,
    // we want to re-run this file. (preserving default behavior)
    println!("cargo:rerun-if-changed=./");
    Ok(())
}
