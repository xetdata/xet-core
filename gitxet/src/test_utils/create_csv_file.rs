use clap::Parser;
use rand::distributions::{Alphanumeric, Uniform};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

/// Write a file filled with pseudorandom data to a specified path.  The data written is
/// deterministic based on the column format and input seed.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct WriteRandomFileArgs {
    #[clap(long)]
    pub out: PathBuf,

    #[clap(long)]
    pub columns: String,

    #[clap(long)]
    pub lines: usize,

    #[clap(long)]
    pub header: bool,

    #[clap(long)]
    pub seed: Option<String>,

    #[clap(long)]
    pub block_size: Option<usize>,

    #[clap(long)]
    pub append: bool,
}

fn main() {
    let args = WriteRandomFileArgs::parse();

    // Initialize RNG with either provided seed or randomly
    let mut rng = if let Some(seed) = args.seed {
        let seed_data = md5::compute(seed);
        let mut seed_array = [0u8; 32];
        seed_array[..seed_data.len()].copy_from_slice(&seed_data[..]);
        StdRng::from_seed(seed_array)
    } else {
        StdRng::from_entropy()
    };

    let file = OpenOptions::new()
        .write(true)
        .create(!args.append)
        .append(args.append)
        .open(&args.out)
        .expect("Failed to open file");

    let mut writer = BufWriter::new(file);

    // Write header if required
    if args.header {
        let headers: Vec<String> = (1..=args.columns.len())
            .map(|i| format!("Col{:02}", i))
            .collect();
        writeln!(writer, "{}", headers.join(",")).expect("Failed to write header");
    }

    // Define column generators based on the format string
    let column_generators: Vec<fn(&mut StdRng) -> String> = args
        .columns
        .chars()
        .map(|ch| match ch {
            'n' => |rng: &mut StdRng| rng.sample(Uniform::new(0, 1000001)).to_string(),
            's' => |rng: &mut StdRng| {
                rng.sample_iter(&Alphanumeric)
                    .take(8)
                    .map(|c| c as char)
                    .collect::<String>()
            },
            _ => panic!("Unsupported column type: {}", ch),
        })
        .collect();

    let block_size = args.block_size.unwrap_or(args.lines);
    let mut blocks: Vec<Vec<String>> = Vec::new();

    // Generate unique block lines
    for _ in 0..block_size {
        let line: Vec<String> = column_generators.iter().map(|gen| gen(&mut rng)).collect();
        blocks.push(line);
    }

    // Repeat block lines until total number of lines is reached
    let mut line_count = 0;
    while line_count < args.lines {
        for line in &blocks {
            if line_count >= args.lines {
                break;
            }
            writeln!(writer, "{}", line.join(",")).expect("Failed to write line");
            line_count += 1;
        }
    }

    writer.flush().expect("Failed to flush to file");
}
