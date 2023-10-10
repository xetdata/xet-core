#[macro_use]
extern crate afl;
extern crate gitxetcore;
extern crate pointer_file;

use std::{path::{Path, PathBuf}, io::Read};

use gitxetcore::{async_file_iterator::AsyncFileIterator, constants::GIT_MAX_PACKET_SIZE, data_processing_v2::PointerFileTranslatorV2};
use pointer_file::PointerFile;

fn main() {
    fuzz!(|input_bytes: &[u8]| { // test smudge passthrough
        let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

        rt.block_on(async {
            let input = std::io::Cursor::new(input_bytes.clone());
            let async_input = AsyncFileIterator::new(input, GIT_MAX_PACKET_SIZE);

            // make sure this dir exists ahead of time!
            // might want to use a ramdisk mounted to that dir to speed up fuzzing.
            let stagedir = Path::new("/tmp/fuzz_mdb"); 

            // make a translator, disabling small file
            let mut repo = PointerFileTranslatorV2::new_temporary(stagedir)
                .await
                .unwrap();
            repo.small_file_threshold = 0;

            // clean the file
            let cleaned = repo.clean_file(&PathBuf::new(), async_input).await.unwrap();
            repo.finalize_cleaning().await.unwrap();
            // check that the cleaned file parses correctly
            let ptr_file = PointerFile::init_from_string(std::str::from_utf8(&cleaned).unwrap(), "");
            assert!(ptr_file.is_valid());

            {
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
                assert_eq!(input_bytes, smudged_bytes);
            }
            {
                let clean_cursor = std::io::Cursor::new(cleaned.clone());
                let async_clean_input = AsyncFileIterator::new(clean_cursor, GIT_MAX_PACKET_SIZE);
                // smudge with passthrough flagged
                // Since this is a valid pointer file, we should smudge as expected
                let mut smudged = std::io::Cursor::new(Vec::new());
                repo.smudge_file(&PathBuf::new(), async_clean_input, &mut smudged, true, None)
                    .await
                    .unwrap();
                // result should be identical
                smudged.set_position(0);
                let mut smudged_bytes: Vec<u8> = Vec::new();
                smudged.read_to_end(&mut smudged_bytes).unwrap();
                assert_eq!(input_bytes, smudged_bytes);
            }
        });
    });
}
