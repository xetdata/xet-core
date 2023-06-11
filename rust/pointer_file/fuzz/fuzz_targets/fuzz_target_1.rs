#![no_main]
use libfuzzer_sys::fuzz_target;
use pointer_file::PointerFile;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let empty_string = "".to_string();
        let input = s.to_string();
        let pointer_file = PointerFile::init_from_string(&input, &empty_string);
        if pointer_file.is_valid() {
            let serialized = pointer_file.to_string();
            let deserialized = PointerFile::init_from_string(&serialized, &empty_string);
            assert_eq!(pointer_file, deserialized);
        }
    }
});
