use std::env;
use std::fs::File;
use std::io::Read;
use std::os::raw::c_void;

use libc::O_RDONLY;

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
    let fsize = file.metadata().unwrap().len();
    println!("file size : {fsize}");
    let mut contents = String::new();

    // Read the file contents into a string
    file.read_to_string(&mut contents).unwrap();
    // let file = unsafe { libc::open(file_path.as_ptr() as *const i8, O_RDONLY) };
    // let mut contents = vec![0u8; 6];
    // let nbytes = unsafe { libc::read(file, contents.as_mut_ptr() as *mut c_void, 7) };
    // println!("Read {nbytes} bytes");

    // Print the file contents to stdout
    //print!("{}", String::from_utf8(contents).unwrap());
    print!("{}", contents);
}
