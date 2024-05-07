use clap::Parser;
use md5;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

/// Write a file filled with pseudorandom data to a specified path.  The data written is
/// deterministic based on the size, block size, and input seed.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct WriteRandomFileArgs {
    #[clap(long)]
    pub out: PathBuf,

    #[clap(long)]
    pub size: usize,

    #[clap(long)]
    pub seed: Option<String>,

    #[clap(long)]
    pub block_size: Option<usize>,

    /// If set, appends data to the file if it already exists.
    #[clap(long)]
    pub append: bool,

    /// If set, only outputs valid ascii
    #[clap(long)]
    pub ascii: bool,
}

fn main() {
    let args = WriteRandomFileArgs::parse();

    // Initialize RNG with either provided seed or randomly
    let mut rng = if let Some(seed) = args.seed {
        let mut hasher = md5::Context::new();
        hasher.consume(seed);
        let seed_bytes = hasher.compute();
        let mut seed_data = [0u8; 32];
        seed_data[..seed_bytes.len()].copy_from_slice(&seed_bytes[..]);
        StdRng::from_seed(seed_data)
    } else {
        StdRng::from_entropy()
    };

    // Handle file writing mode
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .append(args.append)
        .open(&args.out)
        .expect("Failed to open file");

    let mut file = BufWriter::new(file);
    let mut hasher = md5::Context::new();

    let data_block_size = args.block_size.unwrap_or(4096);
    let mut data_block = vec![0u8; data_block_size];

    let mut written_size = 0;

    let only_ascii = args.ascii;
    let mut refresh_data = move |data: &mut [u8]| {
        rng.fill(&mut data[..]);
        if only_ascii {
            for c in data {
                if *c >= 127 {
                    *c %= 127;
                }
                if *c < 32 && *c != b'\n' {
                    *c += 32;
                }
            }
        }
    };

    refresh_data(&mut data_block[..]);

    while written_size < args.size {
        // New data if we aren't writing blocks
        if args.block_size.is_none() {
            refresh_data(&mut data_block[..]);
        }

        let write_size = data_block.len().min(args.size - written_size);
        file.write_all(&data_block[..write_size])
            .expect("Failed to write data");
        hasher.consume(&data_block[..write_size]);
        written_size += write_size;
    }

    file.flush().expect("Failed to flush to file");
    let checksum = hasher.compute();
    println!("{:x}", checksum);
}
