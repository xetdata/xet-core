use ref_cast::RefCast;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use xet_error::Error;

#[derive(Error, Debug)]
#[error("failed to read '{file}'")]
struct StructPathBuf {
    file: PathBuf,
}

#[derive(Error, Debug, RefCast)]
#[repr(C)]
#[error("failed to read '{file}'")]
struct StructPath {
    file: Path,
}

#[derive(Error, Debug)]
enum EnumPathBuf {
    #[error("failed to read '{0}'")]
    Read(PathBuf),
}

fn assert<T: Display>(expected: &str, value: T) {
    assert_eq!(expected, value.to_string());
}

#[test]
fn test_display() {
    let path = Path::new("/xet_error");
    let file = path.to_owned();
    assert("failed to read '/xet_error'", StructPathBuf { file });
    let file = path.to_owned();
    assert("failed to read '/xet_error'", EnumPathBuf::Read(file));
    assert("failed to read '/xet_error'", StructPath::ref_cast(path));
}
