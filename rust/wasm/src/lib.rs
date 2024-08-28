use std::cell::RefCell;
use std::io::Read;
use std::path::{Path, PathBuf};

use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::wasm_bindgen;
use web_sys::js_sys::Uint8Array;

use gitxetcore::constants::GIT_MAX_PACKET_SIZE;
use gitxetcore::data::{PointerFile, PointerFileTranslatorV2};
use gitxetcore::stream::data_iterators::AsyncFileIterator;

// A macro to provide `println!(..)`-style syntax for `console.log` logging.
#[macro_export]
macro_rules! log {
    ( $( $t:tt )* ) => {
        web_sys::console::log_1(&format!( $( $t )* ).into());
    }
}

thread_local! {
    static repo_const: RefCell<Option<PointerFileTranslatorV2>> = RefCell::new(None);
}

#[wasm_bindgen]
pub async fn clean(data: Uint8Array) -> String {
    console_error_panic_hook::set_once();
    let content = Uint8Array::new(&data).to_vec();
    log!("{:?}", content);
    let input = std::io::Cursor::new(content);
    let async_input = AsyncFileIterator::new(input, GIT_MAX_PACKET_SIZE);

    let mut repo = PointerFileTranslatorV2::new_temporary(Path::new(""))
        .await
        .unwrap();
    repo.small_file_threshold = 0;
    let cleaned = repo.clean_file(&PathBuf::new(), async_input).await.unwrap();
    repo.finalize_cleaning().await.unwrap();
    let ptr_file = PointerFile::init_from_string(std::str::from_utf8(&cleaned).unwrap(), "");
    log!("ptr_file: {ptr_file}");

    let clean_cursor = std::io::Cursor::new(cleaned.clone());
    let async_clean_input = AsyncFileIterator::new(clean_cursor, GIT_MAX_PACKET_SIZE);
    // smudge without passthrough flagged
    let mut smudged = std::io::Cursor::new(Vec::new());
    repo.smudge_file(
        &PathBuf::new(),
        async_clean_input,
        &mut smudged,
        false,
        None,
    )
        .await
        .unwrap();
    // result should be identical
    smudged.set_position(0);
    let mut smudged_bytes: Vec<u8> = Vec::new();
    smudged.read_to_end(&mut smudged_bytes).unwrap();
    log!("smudged size {}", smudged_bytes.len());
    // return Result(JsValue::from_str(ptr_file.to_string().as_str()), JsValue::from(smudged_bytes));
    repo_const.set(Some(repo));
    return ptr_file.to_string();
}

#[wasm_bindgen]
pub async fn smudge(data: JsValue) -> String {
    let ptr = data.as_string().unwrap();
    let ptr_file = PointerFile::init_from_string(ptr.as_str(), "");
    log!("ptr_file: {} is valid {}", ptr_file, ptr_file.is_valid());

    let mut repo = repo_const.take().unwrap();

    let mut smudged = std::io::Cursor::new(Vec::new());
    repo.smudge_file_from_pointer(&PathBuf::new(), &ptr_file, &mut smudged, None).await.unwrap();
    smudged.set_position(0);
    let mut smudged_bytes: Vec<u8> = Vec::new();
    smudged.read_to_end(&mut smudged_bytes).unwrap();
    log!("smudged {:?}", smudged_bytes);
    return String::from_utf8(smudged_bytes).unwrap();
}
