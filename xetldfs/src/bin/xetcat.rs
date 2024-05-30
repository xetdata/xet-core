use std::env;
use std::fs::File;
use std::io::{self, Read};

#[no_mangle]
#[export_name = "xetcat_interception_test"]
pub extern "C" fn xetcat_interception_test() -> i32 {
    eprintln!("xetcat hook: Not intercepted.");
    9999
}

fn main() {
    if xetcat_interception_test() != 0 {
        std::process::exit(9999);
    }

    // Get the command line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <file_path>", args[0]);
        std::process::exit(1);
    }

    let file_path = &args[1];

    // Open the file
    let mut file = File::open(file_path).unwrap();
    let mut contents = String::new();

    // Read the file contents into a string
    file.read_to_string(&mut contents).unwrap();

    // Print the file contents to stdout
    print!("{}", contents);
}
