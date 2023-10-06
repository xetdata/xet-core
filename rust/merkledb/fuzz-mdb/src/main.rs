#[macro_use]
extern crate afl;
extern crate gitxetcore;

use std::{path::PathBuf, io::Read};

use gitxetcore::{async_file_iterator::AsyncFileIterator, constants::GIT_MAX_PACKET_SIZE, data_processing_v1::PointerFileTranslatorV1};
use tempfile::TempDir;

fn main() {
    fuzz!(|input_bytes: &[u8]| { // test smudge passthrough
        let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

        rt.block_on(async {
            let input = std::io::Cursor::new(input_bytes.clone());
            let async_input = AsyncFileIterator::new(input, GIT_MAX_PACKET_SIZE);

            // make a translator
            let stagedir = TempDir::new().unwrap();
            let repo = PointerFileTranslatorV1::new_temporary(stagedir.path());

            // smudge the input with passthrough flag set
            let mut output = std::io::Cursor::new(Vec::new());
            repo.smudge_file(&PathBuf::new(), async_input, &mut output, true, None)
                .await
                .unwrap();
            // result should be identical
            let mut output_bytes: Vec<u8> = Vec::new();
            output.set_position(0);
            output.read_to_end(&mut output_bytes).unwrap();
            assert_eq!(input_bytes, output_bytes);
        });
    });
}
