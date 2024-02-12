// Joins two paths together when paths are specified as strings.
pub fn xet_join(source: impl Into<&str>, dest: impl Into<&str>) -> String {
    let source = source.into().rstrip('/');
    format!("{}/{}", source.into(), dest.into())
}
