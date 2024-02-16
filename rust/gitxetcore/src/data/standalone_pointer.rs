use super::PointerFileTranslatorV1;
use crate::config::XetConfig;
use crate::constants::GIT_MAX_PACKET_SIZE;
use crate::errors;
use crate::stream::data_iterators::AsyncFileIterator;
use bincode::Options;
use merkledb::*;
use serde::{Deserialize, Serialize};
use std::io;

const STANDALONE_POINTER_VERSION: u64 = 0;
const STANDALONE_POINTER_HEADER: [u8; 8] = *b"XET_SPTR";
/// This defines a fully standalone pointer file layout which is basically
/// a MerkleDB stashed together a pointer file.
/// `file_to_standalone_pointer` can be used to convert any single file to
/// a binary serialization of a standalone pointer.
/// and `standalone_pointer_to_file` can be used for the reverse
#[derive(Serialize, Deserialize, Clone)]
struct StandalonePointer {
    /// Must be STANDALONE_POINTER_HEADER
    header: [u8; 8],
    /// Must be STANDALONE_POINTER_VERSION
    version: u64,
    /// The number of recursive rounds of encoding the StandalonePointer.
    /// i.e. If you encode a make a StandalonePointer of a StandalonePointer,
    /// this number should increment. And the decode should flatten it.
    /// TODO: Not implemented at the moment. But leaving it in the format
    /// spec so we don't need to bump a format version when we do support it.
    recursion: u64,
    mdb: MerkleMemDB,
    pointer_bytes: Vec<u8>,
}

/// Converts an input file to a standalone pointer
pub async fn file_to_standalone_pointer(
    config: &XetConfig,
    read: impl io::Read + Send + Sync + 'static,
    write: impl io::Write,
) -> errors::Result<()> {
    let p = PointerFileTranslatorV1::from_config_ephemeral(config).await?;

    let file_iterator = AsyncFileIterator::new(read, GIT_MAX_PACKET_SIZE);
    let pointer_bytes = p
        .clean_file(&std::path::PathBuf::from("merkledb.db"), file_iterator)
        .await?;

    p.finalize_cleaning().await?;

    let output = StandalonePointer {
        header: STANDALONE_POINTER_HEADER,
        version: STANDALONE_POINTER_VERSION,
        recursion: 0,
        mdb: p.mdb.lock().await.clone(),
        pointer_bytes,
    };

    let options = bincode::DefaultOptions::new().with_fixint_encoding();
    options.serialize_into(write, &output).map_err(|_| {
        errors::GitXetRepoError::Other("Unable to serialize standalone pointer".into())
    })?;

    Ok(())
}

/// Converts an standalone pointer to a file
pub async fn standalone_pointer_to_file(
    config: &XetConfig,
    read: impl io::Read + Send + Sync,
    mut write: impl io::Write,
) -> errors::Result<()> {
    let options = bincode::DefaultOptions::new().with_fixint_encoding();
    let mut input: StandalonePointer = options.deserialize_from(read).map_err(|_| {
        errors::GitXetRepoError::DataParsingError("Unable to deserialize standalone pointer".into())
    })?;
    if input.header != STANDALONE_POINTER_HEADER {
        return Err(errors::GitXetRepoError::DataParsingError(format!(
            "standalone pointer header incorrect. Expected {}, got {}",
            String::from_utf8_lossy(&STANDALONE_POINTER_HEADER),
            String::from_utf8_lossy(&input.header)
        )));
    }
    if input.version != 0 {
        return Err(errors::GitXetRepoError::DataParsingError(format!(
            "standalone pointer version incorrect. Expected {}, got {}. Please upgrade git-xet.",
            STANDALONE_POINTER_VERSION, input.version
        )));
    }
    if input.recursion != 0 {
        return Err(errors::GitXetRepoError::DataParsingError(format!(
            "Recursion level {} > 0 not supported",
            input.recursion
        )));
    }
    input.mdb.rebuild_hash();
    let mut mod_config = config.clone();
    mod_config.force_no_smudge = false;

    let p = PointerFileTranslatorV1::from_config_ephemeral(&mod_config).await?;
    *(p.mdb.lock().await) = input.mdb;

    let file_iterator = AsyncFileIterator::new(&input.pointer_bytes[..], GIT_MAX_PACKET_SIZE);

    p.smudge_file(
        &std::path::PathBuf::default(),
        file_iterator,
        &mut write,
        true,
        None,
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use tempfile::TempDir;

    use rand::Rng;

    use super::*;

    #[tokio::test]
    async fn test_small_standalone_pointer_round_trip() {
        // make a translator
        let stagedir = TempDir::new().unwrap();
        let config = XetConfig {
            staging_path: Some(stagedir.path().into()),
            ..Default::default()
        };
        // build an input of "hello world"
        let input_bytes: Vec<u8> = "hello world".bytes().collect();
        let input = io::Cursor::new(input_bytes.clone());
        let mut pointer = io::Cursor::new(Vec::new());
        let mut output = io::Cursor::new(Vec::new());

        file_to_standalone_pointer(&config, input, &mut pointer)
            .await
            .unwrap();
        eprintln!("pointer len {}", pointer.position());
        pointer.set_position(0);

        standalone_pointer_to_file(&config, pointer, &mut output)
            .await
            .unwrap();
        let mut output_bytes: Vec<u8> = Vec::new();
        output.set_position(0);
        output.read_to_end(&mut output_bytes).unwrap();
        assert_eq!(input_bytes, output_bytes);
    }

    #[tokio::test]
    async fn test_large_standalone_pointer_round_trip() {
        // make a translator
        let stagedir = TempDir::new().unwrap();
        let config = XetConfig {
            staging_path: Some(stagedir.path().into()),
            ..Default::default()
        };
        // build an input of "hello world"
        let mut input_bytes: Vec<u8> = vec![0; 1024 * 1024];
        let mut rng = rand::thread_rng();
        input_bytes.iter_mut().for_each(|b| {
            *b = rng.gen();
        });
        let input = io::Cursor::new(input_bytes.clone());
        let mut pointer = io::Cursor::new(Vec::new());
        let mut output = io::Cursor::new(Vec::new());

        file_to_standalone_pointer(&config, input, &mut pointer)
            .await
            .unwrap();
        eprintln!("pointer len {}", pointer.position());
        pointer.set_position(0);

        standalone_pointer_to_file(&config, pointer, &mut output)
            .await
            .unwrap();
        let mut output_bytes: Vec<u8> = Vec::new();
        output.set_position(0);
        output.read_to_end(&mut output_bytes).unwrap();
        assert_eq!(input_bytes, output_bytes);
    }
}
