// tests/integration_test.rs
use std::fs::File;
use std::io::Write;
use std::process::Command;
use tempdir::TempDir;

pub fn cat_file(...) {
    let output = Command::new(env!("CARGO_BIN_EXE_xetcat"))
        .arg(&test_file_path)
        .env("DYLD_INSERT_LIBRARIES", &lib_file)
        .env("LD_PRELOAD", &lib_file)
        .stderr(std::process::Stdio::inherit())
        .output()
        .expect("Failed to execute bash command");
}
pub fn append_to_file(...) {}

pub fn write_to_file(...) {}

#[test]
fn test_ld_preload_integration() {
    // Create a temporary directory
    let dir = TempDir::new("test_ld_preload").expect("Could not create temp dir");

    // Create a temporary file with the proper filename ending
    let test_file_path = dir.path().join("example_TEST_FILE");
    let mut test_file = File::create(&test_file_path).expect("Could not create test file");

    // Write some data to the file
    writeln!(test_file, "Initial data").expect("Could not write to test file");

    // Get the library name from the environment variable
    let lib_name = env!("CARGO_PKG_NAME");

    // Construct the library file name based on the target OS
    let lib_file = if cfg!(target_os = "linux") {
        format!("lib{}.so", lib_name)
    } else if cfg!(target_os = "macos") {
        format!("lib{}.dylib", lib_name)
    } else {
        panic!("Unsupported target OS");
    };
    // Path to the compiled library

    // Run `bash -c 'cat <filename>'` in a subprocess with LD_PRELOAD

    if !output.status.success() {
        panic!(
            "Command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Check the output
    let output_str = String::from_utf8_lossy(&output.stdout);
    assert!(output_str.ends_with(":SUCCESSFUL:"));
}
