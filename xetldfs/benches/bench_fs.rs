use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use lazy_static::lazy_static;
use std::fs::OpenOptions;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

mod setup;

use setup::{setup_some_pointer_files, setup_some_regular_files, BenchRepo};

fn is_interposed(envname: &str) -> bool {
    !std::env::var_os(envname)
        .and_then(|osstr| osstr.into_string().ok())
        .unwrap_or_default()
        .is_empty()
}

lazy_static! {
    static ref IS_INTERPOSED: AtomicBool = {
        #[cfg(target_os = "linux")]
        let interposed = is_interposed("LD_PRELOAD");
        #[cfg(target_os = "macos")]
        let interposed = is_interposed("DYLD_INSERT_LIBRARIES");

        AtomicBool::new(interposed)
    };
    static ref PREVENT_COMPILER_OPT: AtomicUsize = AtomicUsize::new(0);
}

const DEFAULT_NUM_FILES: usize = 100;
const DEFAULT_SMALL_FILE_SIZE: usize = 100;
const DEFAULT_LARGE_FILE_SIZE: usize = 300_000;
const VARYING_FILE_SIZE: &[usize] = &[100, 1_000, 10_000, 100_000, 200_000, 500_000, 1_000_000];
const VARYING_NUM_FILES: &[usize] = &[1, 10, 50, 100];
const READ_BLOCK_SIZE: usize = 1_024;

fn benchmark_file_ops_varying_file_size<M, F, G>(
    c: &mut Criterion,
    name: &str,
    setup_fn: F,
    file_fn: G,
) where
    M: Fn() -> (BenchRepo, Vec<PathBuf>),
    F: Fn(usize, usize) -> M,
    G: Fn((BenchRepo, Vec<PathBuf>)) + Copy,
{
    let mut group = c.benchmark_group(format!("{name}_{DEFAULT_NUM_FILES}_x_varying_file_size"));
    for &content_size in VARYING_FILE_SIZE {
        group.throughput(Throughput::Bytes(content_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(content_size),
            &content_size,
            |b, &size| {
                b.iter_batched(
                    setup_fn(DEFAULT_NUM_FILES, size),
                    file_fn,
                    BatchSize::SmallInput,
                )
            },
        );
    }
    group.finish();
}

fn benchmark_file_ops_varying_num_files<M, F, G>(
    c: &mut Criterion,
    name: &str,
    setup_fn: F,
    file_fn: G,
) where
    M: Fn() -> (BenchRepo, Vec<PathBuf>),
    F: Fn(usize, usize) -> M,
    G: Fn((BenchRepo, Vec<PathBuf>)) + Copy,
{
    let mut group = c.benchmark_group(format!(
        "{name}_varying_num_files_x_{DEFAULT_SMALL_FILE_SIZE}"
    ));
    for &num_files in VARYING_NUM_FILES {
        group.throughput(Throughput::Elements(num_files as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_files),
            &num_files,
            |b, &n| {
                b.iter_batched(
                    setup_fn(n, DEFAULT_SMALL_FILE_SIZE),
                    file_fn,
                    BatchSize::SmallInput,
                )
            },
        );
    }
    group.finish();

    let mut group = c.benchmark_group(format!(
        "{name}_varying_num_files_x_{DEFAULT_LARGE_FILE_SIZE}"
    ));
    for &num_files in VARYING_NUM_FILES {
        group.throughput(Throughput::Elements(num_files as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_files),
            &num_files,
            |b, &n| {
                b.iter_batched(
                    setup_fn(n, DEFAULT_LARGE_FILE_SIZE),
                    file_fn,
                    BatchSize::SmallInput,
                )
            },
        );
    }
    group.finish();
}

fn benchmark_file_ops<M, F, G>(c: &mut Criterion, name: &str, setup_fn: F, file_fn: G)
where
    M: Fn() -> (BenchRepo, Vec<PathBuf>),
    F: Fn(usize, usize) -> M + Copy,
    G: Fn((BenchRepo, Vec<PathBuf>)) + Copy,
{
    benchmark_file_ops_varying_file_size(c, name, setup_fn, file_fn);
    benchmark_file_ops_varying_num_files(c, name, setup_fn, file_fn);
}

fn open_files_for_truncate_write((_bench_repo, files): (BenchRepo, Vec<impl AsRef<Path>>)) {
    let mut prevent_compiler_opt = 0;
    for f in files {
        let f = f.as_ref();
        let f = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(f)
            .unwrap();
        prevent_compiler_opt += (&f) as *const _ as usize;
    }
    PREVENT_COMPILER_OPT.fetch_add(prevent_compiler_opt, Ordering::Relaxed);
}

fn open_files_for_nontruncate_write((bench_repo, files): (BenchRepo, Vec<impl AsRef<Path>>)) {
    let mut prevent_compiler_opt = 0;
    for f in files {
        let f = f.as_ref();

        // Not xetldfs interposed -> need to `git xet materialize` manually
        if !IS_INTERPOSED.load(Ordering::Relaxed) {
            bench_repo.git_xet_materialize(f);
        }

        let f = OpenOptions::new()
            .write(true)
            .truncate(false)
            .open(f)
            .unwrap();

        prevent_compiler_opt += (&f) as *const _ as usize;
    }
    PREVENT_COMPILER_OPT.fetch_add(prevent_compiler_opt, Ordering::Relaxed);
}

fn open_files_for_read((bench_repo, files): (BenchRepo, Vec<impl AsRef<Path>>)) {
    let mut prevent_compiler_opt = 0;
    for f in files {
        let f = f.as_ref();

        // Not xetldfs interposed -> need to `git xet materialize` manually
        if !IS_INTERPOSED.load(Ordering::Relaxed) {
            bench_repo.git_xet_materialize(f);
        }

        let f = OpenOptions::new().read(true).write(false).open(f).unwrap();

        prevent_compiler_opt += (&f) as *const _ as usize;
    }
    PREVENT_COMPILER_OPT.fetch_add(prevent_compiler_opt, Ordering::Relaxed);
}

fn read_file((bench_repo, files): (BenchRepo, Vec<impl AsRef<Path>>)) {
    let mut prevent_compiler_opt = 0;
    for f in files {
        let f = f.as_ref();

        // Not xetldfs interposed -> need to `git xet materialize` manually
        if !IS_INTERPOSED.load(Ordering::Relaxed) {
            bench_repo.git_xet_materialize(f);
        }

        let f = OpenOptions::new().read(true).open(f).unwrap();

        let mut reader = BufReader::new(f);
        let mut buf = [0u8; READ_BLOCK_SIZE];
        while reader.read(&mut buf).unwrap() != 0 {
            prevent_compiler_opt += buf[0] as usize;
        }
    }
    PREVENT_COMPILER_OPT.fetch_add(prevent_compiler_opt, Ordering::Relaxed);
}

fn benchmark_regular_file(c: &mut Criterion) {
    let interposed = IS_INTERPOSED.load(Ordering::Relaxed);

    benchmark_file_ops_varying_num_files(
        c,
        &format!(
            "{}_interposed_open_regular_file_for_write_truncate",
            interposed
        ),
        setup_some_regular_files,
        open_files_for_truncate_write,
    );

    benchmark_file_ops(
        c,
        &format!(
            "{}_interposed_open_regular_file_for_write_nontruncate",
            interposed
        ),
        setup_some_regular_files,
        open_files_for_nontruncate_write,
    );

    benchmark_file_ops(
        c,
        &format!("{}_interposed_open_regular_file_for_read", interposed),
        setup_some_regular_files,
        open_files_for_read,
    );

    benchmark_file_ops(
        c,
        &format!("{}_interposed_read_regular_file", interposed),
        setup_some_regular_files,
        read_file,
    );

    let val = PREVENT_COMPILER_OPT.load(Ordering::Relaxed);
    if val == 0x816981cd85 {
        eprintln!("{val}");
    }
}

fn benchmark_pointer_file(c: &mut Criterion) {
    let interposed = IS_INTERPOSED.load(Ordering::Relaxed);

    benchmark_file_ops_varying_num_files(
        c,
        &format!(
            "{}_interposed_open_pointer_file_for_write_truncate",
            interposed
        ),
        setup_some_pointer_files,
        open_files_for_truncate_write,
    );

    benchmark_file_ops(
        c,
        &format!(
            "{}_interposed_open_pointer_file_for_write_nontruncate",
            interposed
        ),
        setup_some_pointer_files,
        open_files_for_nontruncate_write,
    );

    benchmark_file_ops(
        c,
        &format!("{}_interposed_open_pointer_file_for_read", interposed),
        setup_some_pointer_files,
        open_files_for_read,
    );

    benchmark_file_ops(
        c,
        &format!("{}_interposed_read_pointer_file", interposed),
        setup_some_pointer_files,
        read_file,
    );

    let val = PREVENT_COMPILER_OPT.load(Ordering::Relaxed);
    if val == 0x816981cd85 {
        eprintln!("{val}");
    }
}

criterion_group!(name = bench_regular_file;
    config = Criterion::default().sample_size(10).measurement_time(Duration::from_secs(300));
    targets = benchmark_regular_file);
criterion_group!(name = bench_pointer_file;
    config = Criterion::default().sample_size(10).measurement_time(Duration::from_secs(300));
    targets = benchmark_pointer_file);
criterion_main!(bench_regular_file, bench_pointer_file);
