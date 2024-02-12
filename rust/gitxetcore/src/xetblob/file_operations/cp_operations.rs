fn perform_cp(
    fs_dest: &dyn FileSystem,
    sources: &[String],
    dest: String,
    recursive: bool,
) -> io::Result<()> {
    let dest_path = Path::new(&dest);
    for source in sources {
        let source_path = Path::new(source);
        if source_path.is_dir() {
            if recursive {
                copy_dir_recursively(fs_dest, source_path, dest_path)?;
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Source is a directory but recursive option is not set.",
                ));
            }
        } else {
            let dest_file_path = dest_path.join(source_path.file_name().unwrap());
            fs_dest.copy(source_path, &dest_file_path)?;
        }
    }
    Ok(())
}

fn copy_dir_recursively(fs_dest: &dyn FileSystem, source: &Path, dest: &Path) -> io::Result<()> {
    if !Path::new(dest).exists() {
        // Assuming existence check is external or add a method in FileSystem trait
        fs_dest.create_dir_all(dest)?;
    }
    for entry in WalkDir::new(source).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        let relative_path = path.strip_prefix(source).unwrap();
        let dest_path = dest.join(relative_path);

        if path.is_dir() {
            fs_dest.create_dir_all(&dest_path)?;
        } else {
            if let Some(parent) = dest_path.parent() {
                if !Path::new(parent).exists() {
                    // Assuming existence check is external or add a method in FileSystem trait
                    fs_dest.create_dir_all(parent)?;
                }
            }
            fs_dest.copy(path, &dest_path)?;
        }
    }
    Ok(())
}
