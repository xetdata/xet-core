use lazy_static::lazy_static;
use libxet::git_integration::git_repo_test_tools::TestRepo;
use rand::Rng;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

lazy_static! {
    pub static ref CAS_DIR: TempDir = TempDir::new().unwrap();
    pub static ref CAS_URL: String = format!(
        "local://{}",
        CAS_DIR.path().as_os_str().to_str().unwrap_or_default()
    );
}

pub struct BenchRepo {
    pub inner: TestRepo,
}

impl BenchRepo {
    pub fn new() -> Self {
        std::env::set_var("XET_CAS_SERVER", CAS_URL.as_str());
        let test_repo = TestRepo::new_xet().unwrap();

        Self { inner: test_repo }
    }

    pub fn working_dir(&self) -> PathBuf {
        self.inner.repo.repo_dir.clone()
    }

    // Create files under the repo.
    pub fn prepare_files(&self, files: &[(impl AsRef<Path>, usize)]) {
        for (fname, size) in files {
            self.inner.write_file(fname, 0, *size).unwrap();
        }
        self.inner
            .repo
            .run_git_checked_in_repo("add", &["."])
            .unwrap();
    }

    // Create files under the repo and turn them into pointer files.
    pub fn prepare_pointer_files(&self, files: &[(impl AsRef<Path>, usize)]) {
        self.prepare_files(files);

        self.inner
            .repo
            .run_git_checked_in_repo("xet", &["dematerialize", "."])
            .unwrap();
    }

    pub fn git_xet_materialize(&self, file: impl AsRef<Path>) {
        let file = file.as_ref().to_str().unwrap();
        self.inner
            .repo
            .run_git_checked_in_repo("xet", &["materialize", file])
            .unwrap();
    }
}

pub fn setup_some_regular_files(
    num_files: usize,
    size: usize,
) -> impl Fn() -> (BenchRepo, Vec<PathBuf>) {
    move || {
        let bench_repo = BenchRepo::new();

        let seed: i32 = rand::thread_rng().gen();

        let files = (0..num_files)
            .map(|i| (format!("f{i}_{seed}.rf"), size))
            .collect::<Vec<_>>();

        bench_repo.prepare_files(files.as_slice());

        let files = files
            .into_iter()
            .map(|(f, _)| bench_repo.working_dir().join(f))
            .collect();

        (bench_repo, files)
    }
}

pub fn setup_some_pointer_files(
    num_files: usize,
    size: usize,
) -> impl Fn() -> (BenchRepo, Vec<PathBuf>) {
    move || {
        let bench_repo = BenchRepo::new();

        let seed: i32 = rand::thread_rng().gen();

        let files = (0..num_files)
            .map(|i| (format!("f{i}_{seed}.pf"), size))
            .collect::<Vec<_>>();

        bench_repo.prepare_pointer_files(files.as_slice());

        let files = files
            .into_iter()
            .map(|(f, _)| bench_repo.working_dir().join(f))
            .collect();

        (bench_repo, files)
    }
}
