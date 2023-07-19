use phf::phf_map;
use tracing::log::warn;
use crate::libmagic::LibmagicSummary;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FileTypeInfo {
    pub mime_type: &'static str,
    pub friendly_type: &'static str,
}

static FILE_TYPES: phf::Map<&'static str, FileTypeInfo> = phf_map! {
    "zachext" => FileTypeInfo {
        friendly_type: "Zach was Here, simple",
        mime_type: "application/x-zach-was-here",
    }
};

pub fn get_summary_from_extension(extension: &str) -> LibmagicSummary {
    warn!("Zach was here, extension is {}", extension);
    match FILE_TYPES.get(extension).cloned() {
        Some(type_info) => LibmagicSummary {
            file_type: extension.to_string(),
            file_type_simple: type_info.friendly_type.to_string(),
            file_type_simple_category: "".to_string(), // this field intentionally left blank; unused
            file_type_mime: type_info.mime_type.to_string(),
            buffer: None,
        },
        None => LibmagicSummary {
            file_type: extension.to_string(),
            file_type_simple: "Unknown".to_string(),
            file_type_simple_category: "".to_string(), // this field intentionally left blank; unused
            file_type_mime: "application/octet-stream".to_string(),
            buffer: None,
        },
    }
}