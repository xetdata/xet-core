use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen_futures::JsFuture;
use web_sys::js_sys::{Promise, Uint8Array};

use gitxetcore::data::PointerFileTranslatorV2;
use tempfile::TempDir;

// A macro to provide `println!(..)`-style syntax for `console.log` logging.
#[macro_export]
macro_rules! log {
    ( $( $t:tt )* ) => {
        web_sys::console::log_1(&format!( $( $t )* ).into());
    }
}

#[wasm_bindgen]
pub async fn entry(promise: Promise) {
    console_error_panic_hook::set_once();
    let fut = JsFuture::from(promise);
    let result = fut.await.unwrap();
    let content = Uint8Array::new(&result).to_vec();

    let stagedir = TempDir::new().unwrap();
    let mut repo = PointerFileTranslatorV2::new_temporary(stagedir.path())
        .await
        .unwrap();

    // log!("{:?}", check_passthrough_status(content, 100).await)
    // log!("{:?}", clean_file(content).await);
    std::fs::metadata("//Users/samhorradarn/huggingface/llama.cpp").unwrap();
}
