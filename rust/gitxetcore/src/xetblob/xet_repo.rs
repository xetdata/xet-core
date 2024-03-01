use super::atomic_commit_queries::*;
use super::bbq_queries::*;
use super::file_open_flags::*;
use super::rfile_object::FileContent;
use super::*;
use crate::command::CliOverrides;
use crate::config::remote_to_repo_info;
use crate::config::{ConfigGitPathOption, XetConfig};
use crate::constants::{
    GIT_NOTES_MERKLEDB_V1_REF_NAME, GIT_NOTES_MERKLEDB_V2_REF_NAME, MAX_CONCURRENT_DOWNLOADS,
};
use crate::data::*;
use crate::errors::GitXetRepoError;
use crate::git_integration::*;
use crate::summaries::*;
use anyhow::anyhow;
use cas::gitbaretools::{Action, JSONCommand};
use cas::safeio::write_all_file_safe;
use core::panic;
use git_repo_salt::repo_salt_from_bytes;
use mdb_shard::constants::MDB_SHARD_MIN_TARGET_SIZE;
use mdb_shard::session_directory::consolidate_shards_in_directory;
use mdb_shard::shard_version::ShardVersion;
use merkledb::constants::TARGET_CDC_CHUNK_SIZE;
use merkledb::MerkleMemDB;
use merklehash::MerkleHash;
use remote_shard_interface::GlobalDedupPolicy;
use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempdir::TempDir;
use tokio::sync::Mutex;
use tracing::{debug, error, info};
use url::Url;

const TARGET_SINGLE_COMMIT_MAX_SIZE: usize = 16 * 1024 * 1024;
const XET_REPO_INFO_FILE: &str = ".xetblob";

/// Describes a single Xet Repo and manages the MerkleDB within
pub struct XetRepo {
    remote_base_url: Url,
    config: XetConfig,
    translator: Arc<PointerFileTranslator>,
    bbq_client: BbqClient,
    repo_info: Option<RepoInfo>,
}

enum NewFileSource {
    Oid(String),
    NewFile(Arc<XetWFileObject>),
}

struct XRWTMdbInfoV1 {
    oldmdb: MerkleMemDB,
}

struct XRWTMdbInfoV2 {}

enum XRWTMdbSwitch {
    MdbV1(XRWTMdbInfoV1),
    MdbV2(XRWTMdbInfoV2),
}

/// Describes a write transaction
pub struct XetRepoWriteTransaction {
    remote_base_url: Url,
    config: XetConfig,
    branch: String,
    oldsummaries: WholeRepoSummary,
    files: HashMap<String, NewFileSource>,
    delete_files: HashSet<String>,
    move_files: HashSet<(String, String)>,
    author_name: String,
    author_email: String,
    bbq_client: BbqClient,
    translator: Arc<PointerFileTranslator>,
    mdb: XRWTMdbSwitch,

    #[allow(dead_code)]
    shard_session_dir: TempDir,
}

impl XetRepo {
    /// Open a remote repo using a local path as temporary metadata storage location.
    /// If no xet config is provided, one is automatically loaded from global
    /// environment.
    pub async fn open(
        config: Option<XetConfig>,
        overrides: Option<CliOverrides>,
        remote: &str,
        rootpath: &Path,
        dirname: &str,
        bbq_client: &BbqClient,
    ) -> anyhow::Result<Self> {
        let config = if let Some(config) = config {
            config
        } else {
            XetConfig::new(None, None, ConfigGitPathOption::NoPath)?
        };

        let mut remote = remote.to_string();
        // we automatically append a ".git" so people can just copy the web url
        if (remote.starts_with("http://") || remote.starts_with("https://"))
            && !remote.ends_with(".git")
        {
            remote += ".git";
        }
        let config = config.switch_repo_info(remote_to_repo_info(&remote), overrides.clone())?;

        let remote = config.build_authenticated_remote_url(&remote);

        let url = git_remote_to_base_url(&remote)?;

        // query for MDB version and repo salt
        let (repo_info, raw_response) = get_repo_info(&url, bbq_client).await?;
        let mdb_version: ShardVersion = repo_info.xet.mdb_version.parse()?;

        match mdb_version {
            ShardVersion::V1 => {
                Self::open_v1_repo(
                    Some(config),
                    overrides,
                    &remote,
                    rootpath,
                    dirname,
                    bbq_client,
                )
                .await
            }
            ShardVersion::V2 => {
                let repo_path = rootpath.join(dirname);
                std::fs::create_dir_all(&repo_path)?;
                // check if there are already contents under the directory and
                // if it represents the same repo
                Self::verify_repo_info(&repo_path, &repo_info, &raw_response)?;

                Self::open_v2_repo(Some(config), overrides, &repo_path, bbq_client, repo_info).await
            }
            ShardVersion::Uninitialized => {
                eprintln!("Detect incorrect repo metadata information, please contact the administrator for help.");
                Err(anyhow!("Uninitialized repo"))
            } // impossible and deadly route
        }
    }

    /// Open an existing local V1 repo or clone it to local.
    async fn open_v1_repo(
        config: Option<XetConfig>,
        overrides: Option<CliOverrides>,
        remote: &str,
        clone_rootpath: &Path,
        clone_dirname: &str,
        bbq_client: &BbqClient,
    ) -> anyhow::Result<Self> {
        let clone_path = clone_rootpath.join(clone_dirname);
        if !clone_path.exists() {
            eprintln!("Initializing repository on first access");
            clone_xet_repo(
                config.as_ref(),
                &[
                    "--bare",
                    "-c",
                    // only fetch MDB v1 refs notes.
                    "remote.origin.fetch=+refs/notes/xet/merkledb:refs/notes/xet/merkledb",
                    &remote,
                    clone_dirname,
                ],
                true,
                Some(&clone_rootpath.to_path_buf()),
                false,
                false,
                true,
            )?;
            eprintln!("Initialization complete");
        }

        let mut config = if let Some(cfg) = config {
            cfg.switch_repo_path(
                ConfigGitPathOption::PathDiscover(clone_path.to_path_buf()),
                overrides,
            )?
        } else {
            XetConfig::new(
                None,
                None,
                ConfigGitPathOption::PathDiscover(clone_path.to_path_buf()),
            )?
        };
        // disable staging
        config.staging_path = None;

        XetRepo::open_with_config_and_repo_info(config, None, bbq_client).await
    }

    /// Open an existing local V2 repo.
    async fn open_v2_repo(
        config: Option<XetConfig>,
        overrides: Option<CliOverrides>,
        path: &Path,
        bbq_client: &BbqClient,
        repo_info: RepoInfo,
    ) -> anyhow::Result<Self> {
        let mut config = if let Some(cfg) = config {
            cfg.switch_repo_info(
                remote_to_repo_info(&repo_info.repo.html_url),
                overrides.clone(),
            )?
            .switch_xetblob_path(path, overrides)?
        } else {
            XetConfig::new(None, None, ConfigGitPathOption::NoPath)?
        };
        // disable staging
        config.staging_path = None;

        Self::open_with_config_and_repo_info(config, Some(repo_info), bbq_client).await
    }

    async fn open_with_config_and_repo_info(
        config: XetConfig,
        repo_info: Option<RepoInfo>,
        bbq_client: &BbqClient,
    ) -> anyhow::Result<Self> {
        let remote = if let Some(repo_info) = repo_info.as_ref() {
            repo_info.repo.html_url.clone()
        } else {
            let remotes = config.remote_repo_paths();
            if remotes.is_empty() {
                return Err(anyhow!("No remote defined"));
            }
            // we just pick the 1st remote
            remotes[0].clone()
        };

        if remote.is_empty() {
            return Err(anyhow!(
                "Unable to infer remote. There may be a problem with git configuration"
            ));
        }
        // associate auth and make a bbq base path
        let remote = config.build_authenticated_remote_url(&remote);
        let url = git_remote_to_base_url(&remote)?;

        let mut translator = if let Some(repo_info) = repo_info.as_ref() {
            let repo_salt_raw = repo_info
                .xet
                .repo_salt
                .as_ref()
                .map(base64::decode)
                .ok_or_else(|| {
                    GitXetRepoError::RepoSaltUnavailable(
                        "repo salt not available from repo info".to_owned(),
                    )
                })??;
            let repo_salt = repo_salt_from_bytes(&repo_salt_raw[..])?;
            PointerFileTranslator::v2_from_config(&config, repo_salt).await?
        } else {
            PointerFileTranslator::from_config_in_repo(&config).await?
        };

        if matches!(
            config.global_dedup_query_policy,
            GlobalDedupPolicy::Always | GlobalDedupPolicy::OnDirectAccess
        ) {
            translator.set_enable_global_dedup_queries(true);
        }

        // TODO: make a PointerFileTranslator that does not stage
        let translator = Arc::new(translator);
        Ok(XetRepo {
            remote_base_url: url,
            config,
            translator,
            bbq_client: bbq_client.clone(),
            repo_info,
        })
    }

    fn verify_repo_info(
        path: &Path,
        repo_info: &RepoInfo,
        raw_response: &[u8],
    ) -> anyhow::Result<()> {
        let xet_repo_info_file = path.join(XET_REPO_INFO_FILE);

        let saved_repo_info: Option<RepoInfo> = if xet_repo_info_file.exists() {
            Some(serde_json::de::from_slice(&std::fs::read(
                &xet_repo_info_file,
            )?)?)
        } else {
            None
        };

        let repo_changed = match saved_repo_info {
            // Either:
            // 1. no repo info found; or
            // 2. a V1 repo was deleted and recreated under the same name as a V2 repo; or
            None => true,
            // a V2 repo was deleted and recreated under the same name
            Some(sri) => sri.xet != repo_info.xet,
        };

        // repo changed, delete all contents and write out the repo info
        if repo_changed {
            std::fs::remove_dir_all(path)?;
            std::fs::create_dir_all(path)?;
            write_all_file_safe(&path.join(XET_REPO_INFO_FILE), raw_response)?;
        }

        Ok(())
    }

    /// Performs a git pull to fetch the latest merkledb from the remote git repo
    async fn pull(&self) -> anyhow::Result<()> {
        let repo = GitXetRepo::open(self.config.clone())?;
        eprintln!("Synchronizing with remote");
        repo.sync_remote_to_notes_for_xetblob("origin")?;
        Ok(repo.sync_notes_to_dbs_for_xetblob().await?)
    }

    /// Synchronizes the local MerkleDB with the remote MerkleDB
    pub async fn reload_merkledb(&self) -> anyhow::Result<()> {
        self.pull().await?;
        self.translator.refresh().await?;
        Ok(())
    }

    /// Performs a file listing.
    pub async fn listdir(&self, branch: &str, path: &str) -> anyhow::Result<Vec<DirEntry>> {
        let body = self
            .bbq_client
            .perform_bbq_query(self.remote_base_url.clone(), branch, path)
            .await?;
        if let Ok(res) = serde_json::de::from_slice(&body) {
            Ok(res)
        } else {
            Err(anyhow!("Not a directory"))
        }
    }

    /// Performs a file stat query.
    pub async fn stat(&self, branch: &str, path: &str) -> anyhow::Result<Option<DirEntry>> {
        let response = self
            .bbq_client
            .perform_stat_query(self.remote_base_url.clone(), branch, path)
            .await?;

        let body = match response {
            Some(x) => x,
            None => return Ok(None),
        };
        let mut ret: DirEntry = serde_json::de::from_slice(&body)?;
        // if this is a branch, the name is empty.
        if path.is_empty() {
            ret.name = branch.to_string();
            ret.object_type = "branch".to_string();
        }
        Ok(Some(ret))
    }
    /// Opens a file a for read, returning a XetRFileObject
    /// which provides file read capability
    pub async fn open_for_read(
        &self,
        branch: &str,
        filename: &str,
        flags: Option<FileOpenFlags>,
    ) -> anyhow::Result<XetRFileObject> {
        let body = self
            .bbq_client
            .perform_bbq_query(self.remote_base_url.clone(), branch, filename)
            .await?;

        // check if its a pointer file
        let ptr_file = PointerFile::init_from_string(&String::from_utf8_lossy(&body), filename);
        let content = if ptr_file.is_valid() {
            let hash = ptr_file.hash()?;
            // if I can't derive blocks, reload the merkledb
            let translator = self.translator.clone();
            let mut blocks = translator.derive_blocks(&hash).await;

            // if I can't derive blocks, reload the merkledb
            if blocks.is_err() {
                self.reload_merkledb().await?;
                blocks = translator.derive_blocks(&hash).await;
            }

            // if I still can't derive blocks, this is a problem.
            // print an error and just return the pointer file
            if blocks.is_err() {
                error!("Unable to smudge file at {branch}/{filename}");
                FileContent::Bytes(body.to_vec())
            } else {
                let disable_cache =
                    match flags.unwrap_or(FILE_FLAG_DEFAULT) & FILE_FLAG_NO_BUFFERING {
                        0 => None, // don't change the existing behavior
                        _ => Some(true),
                    };

                let mini_smudger = translator
                    .make_mini_smudger(&PathBuf::default(), blocks.unwrap(), disable_cache)
                    .await?;
                FileContent::Pointer((ptr_file, mini_smudger))
            }
        } else {
            FileContent::Bytes(body.to_vec())
        };

        Ok(XetRFileObject { content })
    }

    pub async fn begin_batched_write(
        self: Arc<Self>,
        branch: &str,
        commit_message: &str,
    ) -> crate::errors::Result<XetRepoOperationBatch> {
        info!(
            "XetRepo: begin_batched_write for repo {} on branch {branch}",
            self.remote_base_url
        );
        XetRepoOperationBatch::new(self.clone(), branch, commit_message).await
    }

    /// Begins a write transaction
    /// author_name and author_email are optional and default to what
    /// is in the config if not provided.
    /// If neither config or parameter is available, an error is thrown.
    pub async fn begin_write_transaction(
        &self,
        branch: &str,
        author_name: Option<&str>,
        author_email: Option<&str>,
    ) -> anyhow::Result<XetRepoWriteTransaction> {
        let author_name = if let Some(name) = author_name {
            name.to_string()
        } else if let Some(name) = self.config.user.name.clone() {
            name
        } else {
            "".to_string()
        };

        let author_email = if let Some(email) = author_email {
            email.to_string()
        } else if let Some(email) = self.config.user.email.clone() {
            email
        } else {
            "".to_string()
        };
        // just check quickly that the branch exists
        let _ = self
            .bbq_client
            .perform_bbq_query(self.remote_base_url.clone(), branch, "")
            .await
            .map_err(|_| anyhow::anyhow!("Branch does not exist"));

        let mut transaction_config = self.config.clone();
        let shard_session_dir =
            TempDir::new_in(transaction_config.merkledb_v2_session, "mdb_session")?;
        transaction_config.merkledb_v2_session = shard_session_dir.path().to_owned();

        // TODO: with this mechanic, this ends up reinitializing the cas, shard stuff,
        // shard client, etc. for each transaction.
        let mut translator = if let Some(repo_info) = self.repo_info.as_ref() {
            let repo_salt_raw = repo_info
                .xet
                .repo_salt
                .as_ref()
                .map(base64::decode)
                .ok_or_else(|| {
                    GitXetRepoError::RepoSaltUnavailable(
                        "repo salt not available from repo info".to_owned(),
                    )
                })??;
            let repo_salt = repo_salt_from_bytes(&repo_salt_raw[..])?;
            PointerFileTranslator::v2_from_config(&transaction_config, repo_salt).await?
        } else {
            PointerFileTranslator::from_config_in_repo(&transaction_config).await?
        };

        translator.set_enable_global_dedup_queries(true);

        let translator = Arc::new(translator);

        // Download all shards for V1, by default none for V2.
        let mut fetch_shards = false;

        if let &PFTRouter::V1(_) = &self.translator.pft {
            fetch_shards = true;
        }

        if let Some(v) = std::env::var_os("XET_FETCH_ALL_SHARDS") {
            fetch_shards = v != "0";
        }

        if fetch_shards {
            self.pull().await?;

            if let &PFTRouter::V2(_) = &self.translator.pft {
                mdb::sync_mdb_shards_from_git(
                    &self.config,
                    &self.config.merkledb_v2_cache,
                    GIT_NOTES_MERKLEDB_V2_REF_NAME,
                    true,
                )
                .await?;
            }
        }

        let oldsummaries = translator.get_summarydb().lock().await.clone();

        let mdb = match &translator.pft {
            PFTRouter::V1(ref p) => XRWTMdbSwitch::MdbV1(XRWTMdbInfoV1 {
                oldmdb: p.mdb.lock().await.clone(),
            }),
            PFTRouter::V2(_) => XRWTMdbSwitch::MdbV2(XRWTMdbInfoV2 {}),
        };

        // we make a new translator
        Ok(XetRepoWriteTransaction {
            remote_base_url: self.remote_base_url.clone(),
            config: transaction_config,
            branch: branch.to_string(),
            oldsummaries,
            files: HashMap::new(),
            delete_files: HashSet::new(),
            move_files: HashSet::new(),
            author_name,
            author_email,
            bbq_client: self.bbq_client.clone(),
            mdb,
            translator,
            shard_session_dir,
        })
    }

    /// Fetches all the shard in the hints corresponding to one or more source endpoints.
    /// The reference files for preparing the dedup are specified by a list of (branch, path)
    /// tuples.
    ///
    /// As a further criteria, only shards that define chunks in the reference files with dedupable size
    /// exceeding min_dedup_byte_threshholds are downloaded.
    pub async fn fetch_hinted_shards_for_dedup(
        &self,
        reference_files: &[(&str, &str)],
        min_dedup_byte_threshhold: usize,
    ) -> anyhow::Result<()> {
        let PFTRouter::V2(ref tr_v2) = &self.translator.pft else {
            return Ok(());
        };

        debug!(
            "fetch_hinted_shards_for_dedup: Called with reference files {:?}.",
            reference_files
        );

        // Go through and fetch all the shards needed for deduplication, building a list of new shards.
        let shard_download_info = Arc::new(Mutex::new(HashMap::<MerkleHash, usize>::new()));
        let shard_download_info_ref = &shard_download_info;

        // Download all the shard hints in parallel.
        let min_dedup_chunk_count = min_dedup_byte_threshhold / TARGET_CDC_CHUNK_SIZE;

        parutils::tokio_par_for_each(
            Vec::from(reference_files),
            MAX_CONCURRENT_DOWNLOADS,
            |(branch, filename), _| async move {
                let shard_download_info = shard_download_info_ref.clone();
                if let Ok(body) = self
                    .bbq_client
                    .perform_bbq_query(self.remote_base_url.clone(), branch, filename)
                    .await
                {
                    debug!("Querying shard hints associated with {filename}");

                    let file_string = std::str::from_utf8(&body).unwrap_or("");

                    let ptr_file =
                        PointerFile::init_from_string(file_string, filename);

                    if ptr_file.is_valid() {
                        let filename = filename.to_owned();

                        info!("fetch_hinted_shards_for_dedup: Retrieving shard hints associated with {filename}");

                        // TODO: strategies to limit this, and limit the number of shards downloaded?
                        let file_hash = ptr_file.hash()?;
                        let shard_list = tr_v2.get_hinted_shard_list_for_file(&file_hash).await?;

                        if !shard_list.is_empty() {
                            let mut downloads = shard_download_info.lock().await;

                            for e in shard_list.entries {
                                if !tr_v2.get_shard_manager().shard_is_registered(&e.shard_hash).await {
                                   *downloads.entry(e.shard_hash).or_default() +=
                                        e.total_dedup_chunks as usize;
                                }
                            }
                        }
                    } else {
                        debug!("Destination for {filename} not a pointer file.");
                    }
                } else {
                    debug!("No destination value found for {filename}");
                }

                Ok(())
            },
        )
        .await
        .map_err(|e| match e {
            parutils::ParallelError::JoinError => {
                anyhow::anyhow!("Join Error")
            }
            parutils::ParallelError::TaskError(e) => e,
        })?;

        // Now, go through and exclude the ones that don't meet a dedup criteria cutoff.
        let shard_download_list: Vec<MerkleHash> = shard_download_info
            .lock()
            .await
            .iter()
            .filter_map(|(k, v)| {
                if *v >= min_dedup_chunk_count {
                    Some(*k)
                } else {
                    None
                }
            })
            .collect();

        let hinted_shards = mdb::download_shards_to_cache(
            &self.config,
            &self.config.merkledb_v2_cache,
            shard_download_list,
        )
        .await?;

        // Register all the new shards.
        tr_v2
            .get_shard_manager()
            .register_shards_by_path(&hinted_shards, true)
            .await?;

        Ok(())
    }
}

impl XetRepoWriteTransaction {
    /// Opens a file a for write, returning a XetWFileObject
    /// which provides file write capability
    pub async fn open_for_write(&mut self, filename: &str) -> anyhow::Result<Arc<XetWFileObject>> {
        let ret = Arc::new(XetWFileObject::new(filename, self.translator.clone()));
        self.files
            .insert(filename.to_string(), NewFileSource::NewFile(ret.clone()));
        Ok(ret)
    }

    /// Current tracked transaction size
    pub async fn transaction_size(&self) -> usize {
        self.files.len() + self.delete_files.len() + self.move_files.len()
    }

    /// copies a file from src_branch/src_path to the target_ath
    pub async fn copy(
        &mut self,
        src_branch: &str,
        src_path: &str,
        target_path: &str,
    ) -> anyhow::Result<()> {
        let stat = self
            .bbq_client
            .perform_stat_query(self.remote_base_url.clone(), src_branch, src_path)
            .await?;
        if stat.is_none() {
            return Err(anyhow!("Source file {src_path} not found"));
        }
        let ent: DirEntry = serde_json::de::from_slice(&stat.unwrap())?;
        self.files
            .insert(target_path.to_string(), NewFileSource::Oid(ent.git_hash));
        Ok(())
    }

    /// deletes a file at a location
    pub async fn delete(&mut self, filename: &str) -> anyhow::Result<()> {
        // if file was one of the files we just created, just delete
        // from the transaction
        if self.files.remove(filename).is_some() {
            return Ok(());
        }
        let _ = self.delete_files.insert(filename.to_string());
        Ok(())
    }

    /// renames a file
    pub async fn mv(&mut self, src_path: &str, dest_path: &str) -> anyhow::Result<()> {
        // if file was one of the files we just created, just rename it
        // inside the transaction
        if let Some(val) = self.files.remove(src_path) {
            self.files.insert(dest_path.to_string(), val);
            return Ok(());
        }
        let stat = self
            .bbq_client
            .perform_stat_query(self.remote_base_url.clone(), &self.branch, src_path)
            .await?;
        if stat.is_none() {
            return Err(anyhow!("Source file {src_path} not found"));
        }
        let _ = self
            .move_files
            .insert((src_path.to_string(), dest_path.to_string()));
        Ok(())
    }

    /// Cleanly cancels a transaction
    pub async fn cancel(self) -> anyhow::Result<()> {
        for i in self.files.iter() {
            if let NewFileSource::NewFile(ref f) = i.1 {
                f.close().await?;
            }
        }
        self.translator.finalize_cleaning().await?;
        Ok(())
    }

    /// Commits all files. All open files are forced to close.
    /// author_name and author_email are optional and default to whatever
    /// is in the config if not provided.
    /// If neither config or parameter is available, an error is thrown.
    pub async fn commit(mut self, commit_message: &str) -> anyhow::Result<()> {
        for i in self.files.iter() {
            if let NewFileSource::NewFile(ref f) = i.1 {
                f.close().await?;
            }
        }
        self.translator.finalize_cleaning().await?;
        // Must call this to flush merkledb to disk before commit!
        self.translator.finalize().await?;

        // we have 3 commits to build
        // 1. The merkledb commits
        // 2. The summarydb commits.
        // 3. The main git commit
        let author_name = self.author_name.clone();
        let author_email = self.author_email.clone();

        self.commit_mdb(&author_name, &author_email, commit_message)
            .await?;

        self.commit_summarydb(&author_name, &author_email, commit_message)
            .await?;

        self.commit_git_objects(&author_name, &author_email, commit_message)
            .await?;

        self.invalidate_bbq_cache().await;
        Ok(())
    }

    async fn commit_mdb(
        &mut self,
        author_name: &str,
        author_email: &str,
        commit_message: &str,
    ) -> Result<(), anyhow::Error> {
        let (newmdbnote, note_ref) = match &mut self.mdb {
            XRWTMdbSwitch::MdbV1(ref mut mdb_info) => {
                debug!("commit_mdb: beginning MDB V1 commit, msg={commit_message}");
                let oldmdb = take(&mut mdb_info.oldmdb);

                let newmdb = match &self.translator.pft {
                    PFTRouter::V1(ref p) => p.mdb.lock().await,
                    PFTRouter::V2(_) => {
                        panic!("Programming error; bad V1/V2 state mismatch.")
                    }
                };

                let mut diffdb = MerkleMemDB::default();
                diffdb.difference(&newmdb, &oldmdb);

                drop(newmdb);
                if diffdb.node_iterator().count() == 1 {
                    debug!("commit_mdb: No difference in db; skipping.");
                    // nothing to commit
                    // the empty DB always has a size of 1
                    return Ok(());
                }
                mdb_info.oldmdb = MerkleMemDB::default();
                (
                    mdbv1::encode_db_to_note(&self.config, diffdb).await?,
                    GIT_NOTES_MERKLEDB_V1_REF_NAME.to_string(),
                )
            }
            XRWTMdbSwitch::MdbV2(_) => {
                let session_dir = &self.config.merkledb_v2_session;
                debug!("commit_mdb: beginning MDB V2 commit, msg={commit_message}, session_dir = {session_dir:?}");

                let merged_shards =
                    consolidate_shards_in_directory(session_dir, MDB_SHARD_MIN_TARGET_SIZE)?;

                if merged_shards.is_empty() {
                    debug!("commit_mdb: No new shards; skipping.");
                    return Ok(());
                }

                let salt = self.translator.repo_salt()?;

                mdb::sync_session_shards_to_remote(
                    &self.config,
                    &create_cas_client(&self.config).await?,
                    merged_shards,
                    salt,
                )
                .await?;

                let Some(newmdbnote) = mdb::create_new_mdb_shard_note(session_dir)? else {
                    return Ok(());
                };

                (newmdbnote, GIT_NOTES_MERKLEDB_V2_REF_NAME.to_string())
            }
        };

        let newmdbnote = base64::encode(newmdbnote);

        debug!("commit_mdb: Writing notes to {note_ref}.");

        let odb = git2::Odb::new()?;
        // 1000 is just an arbitrary priority number with no significance
        // see https://docs.rs/git2/latest/git2/struct.Odb.html#method.add_new_mempack_backend
        odb.add_new_mempack_backend(1000)?;
        let oid = odb.write(git2::ObjectType::Blob, newmdbnote.as_bytes())?;
        let oidstr = oid.to_string();
        let mdbaction = Action {
            action: "upsert".to_string(),
            file_path: oidstr.to_string(),
            previous_path: String::new(),
            execute_filemode: false,
            content: newmdbnote,
        };
        let mdbcommand = JSONCommand {
            author_name: author_name.to_string(),
            author_email: author_email.to_string(),
            branch: note_ref,
            commit_message: commit_message.to_string(),
            actions: vec![mdbaction],
            create_ref: true,
        };
        let mdb_commit_command = serde_json::to_string(&mdbcommand)
            .map_err(|_| anyhow!("Unexpected serialization error for {:?}", mdbcommand))?;

        perform_atomic_commit_query(self.remote_base_url.clone(), &mdb_commit_command).await?;

        // Now perform cleanup since we know the atomic commit above succeeded
        if let XRWTMdbSwitch::MdbV2(_) = self.mdb {
            debug!(
                "commit_mdb: MDBV2: Moving shards to cache dir {:?}.",
                &self.config.merkledb_v2_cache,
            );
            mdb::move_session_shards_to_local_cache(
                &self.config.merkledb_v2_session,
                &self.config.merkledb_v2_cache,
            )
            .await?;
        }
        debug!("commit_mdb: finished.");

        Ok(())
    }
    async fn commit_summarydb(
        &mut self,
        author_name: &str,
        author_email: &str,
        commit_message: &str,
    ) -> Result<(), anyhow::Error> {
        debug!("commit_summarydb: computing difference.");

        let diffsummarydb;
        {
            let current_summarydb_ref = self.translator.get_summarydb();
            let current_summarydb = current_summarydb_ref.lock().await;

            diffsummarydb = whole_repo_summary_difference(&current_summarydb, &self.oldsummaries);

            if diffsummarydb.is_empty() {
                debug!("commit_summarydb: No new file summary information.");
                // nothing to commit
                return Ok(());
            }

            drop(current_summarydb);
        }

        self.oldsummaries = self.translator.get_summarydb().lock().await.clone();

        let newsummarynote = encode_summary_db_to_note(&diffsummarydb)?;
        drop(diffsummarydb);
        let newsummarynote = base64::encode(newsummarynote);
        let odb = git2::Odb::new()?;
        // 1000 is just an arbitrary priority number with no significance
        // see https://docs.rs/git2/latest/git2/struct.Odb.html#method.add_new_mempack_backend
        odb.add_new_mempack_backend(1000)?;
        let oid = odb.write(git2::ObjectType::Blob, newsummarynote.as_bytes())?;
        let oidstr = oid.to_string();
        let summaryaction = Action {
            action: "upsert".to_string(),
            file_path: oidstr.to_string(),
            previous_path: String::new(),
            execute_filemode: false,
            content: newsummarynote,
        };
        let summary_command = JSONCommand {
            author_name: author_name.to_string(),
            author_email: author_email.to_string(),
            branch: "refs/notes/xet/summaries".to_string(),
            commit_message: commit_message.to_string(),
            actions: vec![summaryaction],
            create_ref: true,
        };
        let summary_commit_command = serde_json::to_string(&summary_command)
            .map_err(|_| anyhow!("Unexpected serialization error for {:?}", summary_command))?;
        perform_atomic_commit_query(self.remote_base_url.clone(), &summary_commit_command).await?;
        Ok(())
    }

    async fn commit_git_objects(
        &mut self,
        author_name: &str,
        author_email: &str,
        commit_message: &str,
    ) -> Result<(), anyhow::Error> {
        let mut actions: Vec<Action> = Vec::new();
        for i in self.delete_files.iter() {
            let action = Action {
                action: "delete".to_string(),
                file_path: i.clone(),
                previous_path: String::new(),
                execute_filemode: false,
                content: String::new(),
            };
            actions.push(action);
        }
        for i in self.move_files.iter() {
            let action = Action {
                action: "move".to_string(),
                file_path: i.1.clone(),
                previous_path: i.0.clone(),
                execute_filemode: false,
                content: String::new(),
            };
            actions.push(action);
        }

        let mut total_size = 0;

        // Start out with a boundary at 0.
        let mut commit_boundaries = vec![0];

        // Possibly break this into multiple commits to avoid significant json parsing blocks
        for file_id in self.files.iter() {
            match file_id.1 {
                NewFileSource::Oid(oid) => {
                    let action = Action {
                        action: "upsert".to_string(),
                        file_path: file_id.0.clone(),
                        previous_path: oid.clone(),
                        execute_filemode: false,
                        content: String::new(),
                    };
                    actions.push(action);
                }
                NewFileSource::NewFile(ref f) => {
                    if let Some(contents) = f.closed_state().await {
                        total_size += contents.len();
                        let action = Action {
                            action: "upsert".to_string(),
                            file_path: file_id.0.clone(),
                            previous_path: String::new(),
                            execute_filemode: false,
                            content: contents.iter().map(|b| *b as char).collect::<String>(),
                        };
                        total_size += action.content.len();

                        actions.push(action);

                        if total_size >= TARGET_SINGLE_COMMIT_MAX_SIZE {
                            commit_boundaries.push(actions.len());
                            total_size = 0;
                        }
                    }
                }
            }
        }

        commit_boundaries.push(actions.len());

        for (lb_i, ub_i) in commit_boundaries[..(commit_boundaries.len() - 1)]
            .iter()
            .zip(&commit_boundaries[1..])
        {
            if *lb_i == *ub_i {
                continue;
            }

            let local_actions: Vec<_> = ((*lb_i)..(*ub_i)).map(|i| take(&mut actions[i])).collect();

            let command = JSONCommand {
                author_name: author_name.to_string(),
                author_email: author_email.to_string(),
                branch: self.branch.clone(),
                commit_message: commit_message.to_string(),
                actions: local_actions,
                create_ref: false,
            };
            let git_object_commit_command = serde_json::to_string(&command)
                .map_err(|_| anyhow!("Unexpected serialization error for {:?}", command))?;
            info!("{git_object_commit_command}");

            let remote_base_url = self.remote_base_url.clone();

            // TODO: If this really slow, then it can easily be parallelized.  For now, do it
            // sequentially
            perform_atomic_commit_query(remote_base_url, &git_object_commit_command).await?;
        }

        Ok(())
    }

    async fn invalidate_bbq_cache(&self) {
        for f in &self.files {
            self.bbq_client
                .invalidate_cache(self.remote_base_url.clone(), &self.branch, f.0)
                .await;
        }

        for f in &self.delete_files {
            self.bbq_client
                .invalidate_cache(self.remote_base_url.clone(), &self.branch, f)
                .await;
        }

        for f in &self.move_files {
            self.bbq_client
                .invalidate_cache(self.remote_base_url.clone(), &self.branch, &f.0)
                .await;
        }
    }
}
