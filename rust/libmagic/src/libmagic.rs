use std::{fs::File, io::Read, io::Write, path::Path};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Clone, Debug, Default)]
pub struct LibmagicAnalyzer {
    read_bytes: Vec<u8>,
}

#[cfg(not(target_os = "windows"))]
impl LibmagicAnalyzer {
    pub fn process_chunk(&mut self, chunk: &[u8]) -> anyhow::Result<()> {
        if self.read_bytes.len() < LIBMAGIC_ANALYZER_MAX_BYTES {
            let bytes_to_add = usize::min(
                LIBMAGIC_ANALYZER_MAX_BYTES - self.read_bytes.len(),
                chunk.len(),
            );
            self.read_bytes.extend_from_slice(&chunk[..bytes_to_add]);
        }
        Ok(())
    }

    pub fn finalize(&mut self) -> anyhow::Result<Option<LibmagicSummary>> {
        info!(
            "Getting libmagic summary for chunk of {} bytes",
            self.read_bytes.len()
        );
        if self.read_bytes.is_empty() {
            return Ok(None);
        }
        let mut result = LibmagicSummary::default();
        let mut cookie = libmagic_cookie(CookieFlags::default())?;
        let file_type = cookie.buffer(&self.read_bytes)?;
        let file_type_simple = simple_file_type(&file_type);
        result.file_type = file_type.clone();
        result.file_type_simple = file_type_simple.to_string();
        result.file_type_simple_category = simple_category(file_type_simple).to_string();

        cookie = libmagic_cookie(CookieFlags::MIME)?;
        let file_mime_type = cookie.buffer(&self.read_bytes)?;
        result.file_type_mime = file_mime_type;
        Ok(Some(result))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct LibmagicSummary {
    pub file_type: String,
    pub file_type_simple: String,
    pub file_type_simple_category: String,
    pub file_type_mime: String,

    // A buffer to allow us to add more to the serialized options
    pub buffer: Option<()>,
}

// Produces a "libmagic" summary (libmagic file type results and heuristics on top of that).
// args.file can be a path to any file within the repo.
// Assumes the _real_ file contents are at the given path, not a pointer file.
// The expected use case is that this utility is called during (immediately after?) smudge.
#[cfg(not(target_os = "windows"))]
pub fn print_libmagic_summary(file_path: &Path) -> anyhow::Result<()> {
    let result = summarize_libmagic(file_path)?;
    let content_str = serde_json::to_string_pretty(&result)
        .map_err(|_| anyhow!("Failed to serialize libmagic summary to JSON"))?;
    println!("{content_str}");
    Ok(())
}
#[cfg(target_os = "windows")]
pub fn print_libmagic_summary(_file_path: &Path) -> anyhow::Result<()> {
    bail!("libmagic summarization not supported on Windows")
}

#[cfg(not(target_os = "windows"))]
pub fn print_libmagic_summary_from_reader(file: &mut impl Read) -> anyhow::Result<()> {
    let result = summarize_libmagic_from_reader(file)?;
    let content_str = serde_json::to_string_pretty(&result)
        .map_err(|_| anyhow!("Failed to serialize libmagic summary to JSON"))?;
    println!("{content_str}");
    Ok(())
}
#[cfg(target_os = "windows")]
pub fn print_libmagic_summary_from_reader(_file: &mut impl Read) -> anyhow::Result<()> {
    bail!("libmagic summarization not supported on Windows")
}

#[cfg(not(target_os = "windows"))]
fn libmagic_cookie(flags: CookieFlags) -> anyhow::Result<Cookie> {
    // extract the magic db if necessary
    let temp_directory = std::env::temp_dir();
    let magic_db_path = temp_directory.join("xet-magic-5.42.mgc");
    let magic_db_list = vec![&magic_db_path];
    if !magic_db_path.exists() {
        let magic_db_gzipped_path = temp_directory.join("xet-magic-5.42.mgc.gz");
        let mut magic_db_gzipped_file = File::create(&magic_db_gzipped_path)?;
        magic_db_gzipped_file.write_all(MAGIC_DB_GZIPPED_BYTES)?;
        let _output = std::process::Command::new("gunzip")
            .current_dir(&temp_directory)
            .arg(&magic_db_gzipped_path)
            .output()?;
        if !magic_db_path.exists() {
            return Err(anyhow!("Failed to create database file for libmagic"));
        }
    }

    let cookie = Cookie::open(flags)?;
    cookie.load(&magic_db_list)?;
    Ok(cookie)
}

// Produces a "libmagic" summary (libmagic file type results and heuristics on top of that).
// args.file can be a path to any file within the repo.
// Assumes the _real_ file contents are at the given path, not a pointer file.
// The expected use case is that this utility is called during (immediately after?) smudge.
#[cfg(not(target_os = "windows"))]
pub fn summarize_libmagic(file_path: &Path) -> anyhow::Result<LibmagicSummary> {
    info!("Computing libmagic summary for {:?}", file_path);
    let mut file = File::open(file_path)?;
    summarize_libmagic_from_reader(&mut file)
}

// Produces a "libmagic" summary (libmagic file type results and heuristics on top of that).
// args.file can be a path to any file within the repo.
// Assumes the _real_ file contents are at the given path, not a pointer file.
// The expected use case is that this utility is called during (immediately after?) smudge.
#[cfg(not(target_os = "windows"))]
pub fn summarize_libmagic_from_reader(file: &mut impl Read) -> anyhow::Result<LibmagicSummary> {
    let mut result = LibmagicSummary::default();
    let mut cookie = libmagic_cookie(CookieFlags::default())?;
    let mut buf = vec![];
    file.read_to_end(&mut buf)?;
    let file_type = cookie.buffer(&buf)?;
    let file_type_simple = simple_file_type(&file_type);
    result.file_type = file_type.clone();
    result.file_type_simple = file_type_simple.to_string();
    result.file_type_simple_category = simple_category(file_type_simple).to_string();

    cookie = libmagic_cookie(CookieFlags::MIME)?;
    let file_mime_type = cookie.buffer(&buf)?;
    result.file_type_mime = file_mime_type;
    Ok(result)
}

#[cfg(target_os = "windows")]
pub fn summarize_libmagic_from_reader(file: &mut impl Read) -> anyhow::Result<LibmagicSummary> {
    bail!("libmagic summarization not supported on Windows")
}

/// Determine a "simple" file type from the human-readable `file` string
fn simple_file_type(s: &str) -> &str {
    if s == "data" {
        return "Unknown";
    }
    if s == "very short file (no magic)" {
        return "Unknown";
    }
    if s == "empty" {
        return "Empty File";
    }
    if s.starts_with("JPEG image data") {
        return "JPEG Image";
    }
    if s.starts_with("PNG image data") {
        return "PNG Image";
    }
    if s.starts_with("TIFF image data") {
        return "TIFF Image";
    }
    if s.starts_with("ISO Media, HEIF Image HEVC Main or Main Still Picture Profile") {
        return "HEIC Image";
    }
    if s.starts_with("Canon CR2 raw image data") {
        return "RAW Image";
    }
    if s.starts_with("DICOM medical imaging data") {
        return "DICOM Image";
    }
    if s.starts_with("SVG Scalable Vector Graphics image") {
        return "SVG Image";
    }
    if s.starts_with("NumPy data file") {
        return "NumPy Data File";
    }
    if s.starts_with("ISO Media, Apple iTunes Video (.M4V) Video") {
        return "MP4 Video";
    }
    if s.starts_with("ISO Media, Apple QuickTime movie, Apple QuickTime (.MOV/QT)") {
        return "QuickTime Video";
    }
    if s.starts_with("ASCII text") {
        return "Text Document";
    }
    if s.starts_with("a ascii label") {
        return "Text Document";
    }
    if s.starts_with("Unicode text") {
        return "Text Document";
    }
    if s.starts_with("Objective-C source text") {
        return "Objective-C Source Code";
    }
    if s.starts_with("Python script") {
        return "Python Source Code";
    }
    if s.starts_with("Algol 68 source text") {
        return "Algol 68 Source Code";
    }
    if s.starts_with("c program text") {
        return "C Source Code";
    }
    if s.starts_with("XML 1.0 document text") {
        return "XML Data File";
    }
    if s.starts_with("CSV text") {
        return "CSV Data File";
    }
    if s.starts_with("JSON data") {
        return "JSON Data File";
    }
    if s.starts_with("Hierarchical Data Format (version 5) data") {
        return "HDF5 Data File";
    }
    if s.starts_with("Apple binary property list") {
        return "Apple Binary Property List Data File";
    }
    if s.starts_with("HTML document text") {
        return "HTML Document";
    }
    if s.starts_with("PDP-11 pure executable") {
        return "PDP-11 Executable";
    }
    if s.starts_with("Mach-O 64-bit executable") {
        return "MacOS Executable";
    }
    if s.starts_with("Mach-O 64-bit dynamically linked shared library") {
        return "MacOS Shared Library";
    }
    if s.starts_with("PE32 executable") {
        return "MS Windows Executable";
    }
    if s.starts_with("FLAC audio") {
        return "FLAC Audio";
    }
    if s.starts_with("RIFF (little-endian) data, WAVE audio") {
        return "WAV Audio";
    }
    if s.starts_with("NIST SPHERE") {
        return "NIST SPHERE Audio";
    }
    if s.starts_with("gzip compressed data") {
        return "GZIP Compressed Data";
    }
    if s.starts_with("Microsoft Windows Autorun file") {
        return "MS Windows Autorun File";
    }
    if s.starts_with("MS Windows shortcut") {
        return "MS Windows Shortcut";
    }
    if s.starts_with("MS Windows icon") {
        return "MS Windows Icon";
    }
    if s.starts_with("TrueType Font data") {
        return "TTF Font";
    }
    if s.starts_with("Web Open Font Format") {
        return "WOFF Font";
    }
    if s.starts_with("Embedded OpenType (EOT)") {
        return "EOT Font";
    }
    if s.starts_with("OpenType font data") {
        return "OpenType Font";
    }
    if s.starts_with("Mac OS X icon") {
        return "MacOS Icon";
    }
    if s.starts_with("AppleDouble encoded Macintosh file") {
        return "MacOS Metadata File"; // Often .DS_Store
    }
    if s.starts_with("Apple Desktop Services Store") {
        return "MacOS Metadata File"; // Also often .DS_Store
    }

    // fall back to full type if we don't have a heuristic for it
    s
}

// A higher level categorization meant to be applied to the "simple" file type
fn simple_category(s: &str) -> &str {
    if s.contains("Image") {
        return "Image";
    }
    if s.contains("Icon") {
        return "Image";
    }
    if s.contains("Video") {
        return "Video";
    }
    if s.contains("Audio") {
        return "Audio";
    }
    if s.contains("Source Code") {
        return "Source Code";
    }
    if s.contains("Document") {
        return "Document";
    }
    if s.contains("Executable") {
        return "Binary Executable";
    }
    if s.contains("Shared Library") {
        return "Binary Executable";
    }
    if s.contains("Font") {
        return "Font";
    }
    if s.contains("Data File") {
        return "Structured Data";
    }
    if s.contains("Compressed") {
        return "Compressed Data";
    }
    if s.contains("MacOS Metadata") {
        return "OS-specific Metadata";
    }
    if s.contains("MS Windows") {
        return "OS-specific Metadata";
    }

    // fall back to simple type if we don't have a heuristic for it
    s
}
