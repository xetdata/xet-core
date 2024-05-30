use std::env;
use std::fs::File;
use std::io::Read;

fn main() {
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
