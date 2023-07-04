use super::bbq_queries::*;
use super::rfile_object::FileContent;
use super::*;
use crate::atomic_commit_queries::*;
use anyhow::anyhow;

use cas::gitbaretools::{Action, JSONCommand};
use gitxetcore::command::CliOverrides;
use gitxetcore::config::remote_to_repo_info;
use gitxetcore::config::{ConfigGitPathOption, XetConfig};
use gitxetcore::constants::{GIT_NOTES_MERKLEDB_V1_REF_NAME, GIT_NOTES_MERKLEDB_V2_REF_NAME};
use gitxetcore::data_processing::*;
use gitxetcore::git_integration::*;
use gitxetcore::merkledb_plumb::*;
use gitxetcore::merkledb_shard_plumb::{
    create_new_mdb_shard_note, move_session_shards_to_local_cache, sync_session_shards_to_remote,
};
use gitxetcore::summaries_plumb::*;
use mdb_shard::merging::consolidate_shards_in_directory;
use mdb_shard::shard_file::MDB_SHARD_MIN_TARGET_SIZE;
use merkledb::MerkleMemDB;
use pointer_file::PointerFile;
use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempdir::TempDir;
use tracing::{debug, error, info};
use url::Url;

/// Describes a single Xet Repo and manages the MerkleDB within
pub struct XetRepo {
    remote_base_url: Url,
    config: XetConfig,
    translator: Arc<PointerFileTranslator>,
    bbq_client: BbqClient,

    #[allow(dead_code)]
    shard_session_dir: TempDir,
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
}

impl XetRepo {
    /// Create a new local MerkleDB clone
    /// If no xet config is provided, one is automatically loaded from global
    /// environment.
    pub async fn new(
        config: Option<XetConfig>,
        overrides: Option<CliOverrides>,
        remote: &str,
        clone_rootpath: &Path,
        clone_dirname: &str,
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

        eprintln!("Initializing repository on first access");
        git_repo::GitRepo::clone(
            Some(&config),
            &[
                "--bare",
                "-c",
                // effectivey fetch both MDB v1 and v2 refs notes.
                "remote.origin.fetch=refs/notes/xet/merkledb*:refs/notes/xet/merkledb*",
                "-c",
                // append '*' so git doesn't report error when the reposalt
                // ref note doesn't exist i.e. in MDB v1.
                "remote.origin.fetch=refs/notes/xet/reposalt*:refs/notes/xet/reposalt*",
                &remote,
                clone_dirname,
            ],
            true,
            Some(&clone_rootpath.to_path_buf()),
            false,
            true,
        )?;
        eprintln!("Initialization complete");
        let mut path = clone_rootpath.to_path_buf();
        path.push(clone_dirname);
        XetRepo::open(Some(config), overrides, &path).await
    }

    /// open an existing local MerkleDB clone
    /// If no xet config is provided, one is automatically loaded from global
    /// environment.
    pub async fn open(
        config: Option<XetConfig>,
        overrides: Option<CliOverrides>,
        path: &Path,
    ) -> anyhow::Result<Self> {
        let mut config = if let Some(config) = config {
            config.switch_repo_path(
                ConfigGitPathOption::PathDiscover(path.to_path_buf()),
                overrides,
            )?
        } else {
            XetConfig::new(
                None,
                None,
                ConfigGitPathOption::PathDiscover(path.to_path_buf()),
            )?
        };
        // disable staging
        config.staging_path = None;
        let remotes = config.remote_repo_paths();
        if remotes.is_empty() {
            return Err(anyhow!("No remote defined"));
        }
        // we just pick the 1st remote
        let remote = remotes[0].clone();
        // associate auth and make a bbq base path
        let remote = config.build_authenticated_remote_url(&remote);
        let url = git_remote_to_base_url(&remote)?;
        if !path.exists() {
            return Err(anyhow!("path {path:?} does not exist"));
        }

        let shard_session_dir =
            TempDir::new_in(path.join(config.merkledb_v2_session), "mdb_session")?;
        config.merkledb_v2_session = shard_session_dir.path().strip_prefix(path)?.to_path_buf();

        let translator = PointerFileTranslator::from_config(&config).await?;

        // TODO: make a PointerFileTranslator that does not stage
        let translator = Arc::new(translator);
        Ok(XetRepo {
            remote_base_url: url,
            config,
            translator,
            shard_session_dir,
            bbq_client: BbqClient::new(),
        })
    }

    /// Performs a git pull to fetch the latest merkledb from the remote git repo
    async fn pull(&self) -> anyhow::Result<()> {
        let repo = git_repo::GitRepo::open(self.config.clone())?;
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

    /// Opens a file a for read, returning a XetRFileObject
    /// which provides file read capability
    pub async fn open_for_read(
        &self,
        branch: &str,
        filename: &str,
    ) -> anyhow::Result<XetRFileObject> {
        let body = self
            .bbq_client
            .perform_bbq_query(self.remote_base_url.clone(), branch, filename)
            .await?;

        // check if its a pointer file
        let ptr_file = PointerFile::init_from_string(&String::from_utf8_lossy(&body), filename);
        let content = if ptr_file.is_valid() {
            // if I can't derive blocks, reload the merkledb
            let translator = self.translator.clone();
            let mut blocks = translator.derive_blocks(&ptr_file).await;

            // if I can't derive blocks, reload the merkledb
            if blocks.is_err() {
                self.reload_merkledb().await?;
                blocks = translator.derive_blocks(&ptr_file).await;
            }

            // if I still can't derive blocks, this is a problem.
            // print an error and just return the pointer file
            let ret = if blocks.is_err() {
                error!("Unable to smudge file at {branch}/{filename}");
                FileContent::Bytes(body.to_vec())
            } else {
                let mini_smudger = translator
                    .make_mini_smudger(&PathBuf::default(), blocks.unwrap())
                    .await?;
                FileContent::Pointer((ptr_file, mini_smudger))
            };

            ret
        } else {
            FileContent::Bytes(body.to_vec())
        };

        Ok(XetRFileObject { content })
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

        let translator = Arc::new(PointerFileTranslator::from_config(&self.config).await?);
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
            config: self.config.clone(),
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
        })
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
        let _ = self.delete_files.insert(filename.to_string());
        Ok(())
    }

    /// deletes a file at a location
    pub async fn mv(&mut self, src_path: &str, dest_path: &str) -> anyhow::Result<()> {
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
                    encode_db_to_note(&self.config, diffdb).await?,
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

                sync_session_shards_to_remote(&self.config, merged_shards).await?;

                let Some(newmdbnote) = create_new_mdb_shard_note(session_dir)? else { return Ok(()); };

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
            move_session_shards_to_local_cache(
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
        for i in self.files.iter() {
            match i.1 {
                NewFileSource::Oid(oid) => {
                    let action = Action {
                        action: "upsert".to_string(),
                        file_path: i.0.clone(),
                        previous_path: oid.clone(),
                        execute_filemode: false,
                        content: String::new(),
                    };
                    actions.push(action);
                }
                NewFileSource::NewFile(ref f) => {
                    if let Some(contents) = f.closed_state().await {
                        let action = Action {
                            action: "upsert".to_string(),
                            file_path: i.0.clone(),
                            previous_path: String::new(),
                            execute_filemode: false,
                            content: contents.iter().map(|b| *b as char).collect::<String>(),
                        };
                        actions.push(action);
                    }
                }
            }
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
        let command = JSONCommand {
            author_name: author_name.to_string(),
            author_email: author_email.to_string(),
            branch: self.branch.clone(),
            commit_message: commit_message.to_string(),
            actions,
            create_ref: false,
        };
        let git_object_commit_command = serde_json::to_string(&command)
            .map_err(|_| anyhow!("Unexpected serialization error for {:?}", command))?;
        info!("{git_object_commit_command}");
        perform_atomic_commit_query(self.remote_base_url.clone(), &git_object_commit_command)
            .await?;
        Ok(())
    }
}
