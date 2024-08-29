use common_constants::LOCAL_CAS_SCHEME;

use super::errors::DataProcessingError;
use super::errors::Result;
use crate::config::XetConfig;
use crate::git_integration::git_repo_salt::RepoSalt;
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Debug)]
pub enum Endpoint {
    Server(String),
    FileSystem(PathBuf),
}

#[derive(Debug)]
pub struct Auth {
    pub user_id: String,
    pub login_id: String,
}

#[derive(Debug)]
pub struct CacheConfig {
    pub cache_directory: PathBuf,
    pub cache_size: u64,
    pub cache_blocksize: u64,
}

#[derive(Debug)]
pub struct StorageConfig {
    pub endpoint: Endpoint,
    pub auth: Auth,
    pub prefix: String,
    pub cache_config: Option<CacheConfig>,
    pub staging_directory: Option<PathBuf>,
}

#[derive(Debug)]
pub struct DedupConfig {
    pub repo_salt: Option<RepoSalt>,
    pub small_file_threshold: usize,
    pub global_dedup_policy: GlobalDedupPolicy,
}

#[derive(Debug)]
pub struct RepoInfo {
    pub repo_paths: Vec<String>,
}

#[derive(Debug, Default)]
pub struct SmudgeConfig {
    pub force_no_smudge: bool, // default is false
}

#[derive(PartialEq, Default, Clone, Debug, Copy)]
pub enum FileQueryPolicy {
    /// Query local first, then the shard server.
    #[default]
    LocalFirst,

    /// Only query the server; ignore local shards.
    ServerOnly,

    /// Only query local shards.
    LocalOnly,
}

impl FromStr for FileQueryPolicy {
    type Err = std::io::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "local_first" => Ok(FileQueryPolicy::LocalFirst),
            "server_only" => Ok(FileQueryPolicy::ServerOnly),
            "local_only" => Ok(FileQueryPolicy::LocalOnly),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid file smudge policy, should be one of local_first, server_only, local_only: {}", s),
            )),
        }
    }
}

#[derive(PartialEq, Default, Clone, Debug, Copy)]
pub enum GlobalDedupPolicy {
    /// Never query for new shards using chunk hashes.
    Never,

    /// Only query for new shards when using direct file access methods like `xet cp`
    #[default]
    OnDirectAccess,

    /// Always query for new shards by chunks (not recommended except for testing)
    Always,
}

impl FromStr for GlobalDedupPolicy {
    type Err = std::io::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "never" => Ok(GlobalDedupPolicy::Never),
            "direct_only" => Ok(GlobalDedupPolicy::OnDirectAccess),
            "always" => Ok(GlobalDedupPolicy::Always),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid global dedup query policy, should be one of never, direct_only, always: {}", s),
            )),
        }
    }
}

#[derive(Debug)]
pub struct TranslatorConfig {
    pub file_query_policy: FileQueryPolicy,
    pub cas_storage_config: StorageConfig,
    pub shard_storage_config: StorageConfig,
    pub dedup_config: Option<DedupConfig>,
    pub repo_info: Option<RepoInfo>,
    pub smudge_config: SmudgeConfig,
}

// Temporary helpers
pub async fn translator_config_from(
    xet: &XetConfig,
    repo_salt: Option<RepoSalt>,
) -> Result<TranslatorConfig> {
    let cas_storage_config = cas_storage_config_from(xet).await?;
    let shard_storage_config = shard_storage_config_from(xet).await?;
    let mut dedup_config = dedup_config_from(xet);
    dedup_config.repo_salt = repo_salt;
    let repo_info = repo_info_from(xet)?;

    Ok(TranslatorConfig {
        file_query_policy: xet.file_query_policy,
        cas_storage_config,
        shard_storage_config,
        dedup_config: Some(dedup_config),
        repo_info: Some(repo_info),
        smudge_config: SmudgeConfig {
            force_no_smudge: xet.force_no_smudge,
        },
    })
}

pub async fn cas_storage_config_from(xet: &XetConfig) -> Result<StorageConfig> {
    let cas_endpoint = xet.cas_endpoint().await.map_err(|e| {
        DataProcessingError::CASConfigError(format!("failed to get endpoint: {e:?}"))
    })?;

    let cas_endpoint =
        if let Some(path) = cas_endpoint.strip_prefix(LOCAL_CAS_SCHEME) {
            Endpoint::FileSystem(PathBuf::from_str(path).map_err(|e| {
                DataProcessingError::CASConfigError(format!("invalid endpoint: {e:?}"))
            })?)
        } else {
            Endpoint::Server(cas_endpoint)
        };

    Ok(StorageConfig {
        endpoint: cas_endpoint,
        auth: Auth {
            user_id: xet.user.get_user_id().0,
            login_id: xet.user.get_login_id(),
        },
        prefix: xet.cas.prefix.clone(),
        cache_config: Some(CacheConfig {
            cache_directory: xet.cache.path.clone(),
            cache_size: xet.cache.size,
            cache_blocksize: xet.cache.blocksize.unwrap_or(16 * 1024 * 1024),
        }),
        staging_directory: xet.staging_path.clone(),
    })
}

pub async fn shard_storage_config_from(xet: &XetConfig) -> Result<StorageConfig> {
    let shard_endpoint = xet.cas_endpoint().await.map_err(|e| {
        DataProcessingError::ShardConfigError(format!("failed to get endpoint: {e:?}"))
    })?;

    let shard_endpoint = if let Some(path) = shard_endpoint.strip_prefix(LOCAL_CAS_SCHEME) {
        Endpoint::FileSystem(PathBuf::from_str(path).map_err(|e| {
            DataProcessingError::ShardConfigError(format!("invalid endpoint: {e:?}"))
        })?)
    } else {
        Endpoint::Server(shard_endpoint)
    };

    Ok(StorageConfig {
        endpoint: shard_endpoint,
        auth: Auth {
            user_id: xet.user.get_user_id().0,
            login_id: xet.user.get_login_id(),
        },
        prefix: xet.cas.shard_prefix(),
        cache_config: Some(CacheConfig {
            cache_directory: xet.merkledb_v2_cache.clone(),
            cache_size: 0,
            cache_blocksize: 0,
        }),
        staging_directory: Some(xet.merkledb_v2_session.clone()),
    })
}

pub fn dedup_config_from(xet: &XetConfig) -> DedupConfig {
    DedupConfig {
        repo_salt: Some(Default::default()),
        small_file_threshold: xet.cas.size_threshold,
        global_dedup_policy: xet.global_dedup_query_policy,
    }
}

pub fn repo_info_from(xet: &XetConfig) -> Result<RepoInfo> {
    Ok(RepoInfo {
        repo_paths: vec![xet
            .repo_path()
            .map_err(|e| DataProcessingError::InternalError(format!("{e:?}")))?
            .to_str()
            .unwrap_or_default()
            .to_owned()],
    })
}
