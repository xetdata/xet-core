use super::bbq_queries::*;
use super::rfile_object::FileContent;
use super::*;
use crate::atomic_commit_queries::*;
use anyhow::anyhow;

use cas::gitbaretools::{Action, JSONCommand};
use gitxetcore::command::CliOverrides;
use gitxetcore::config::remote_to_repo_info;
use gitxetcore::config::{ConfigGitPathOption, XetConfig};
use gitxetcore::data_processing::*;
use gitxetcore::data_processing_v1::*;
use gitxetcore::git_integration::*;
use gitxetcore::merkledb_plumb::*;
use gitxetcore::summaries_plumb::*;
use merkledb::MerkleMemDB;
use pointer_file::PointerFile;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};
use url::Url;

/// Describes a single Xet Repo and manages the MerkleDB within
pub struct XetRepo {
    remote_base_url: Url,
    config: XetConfig,
    translator: Mutex<Arc<PointerFileTranslator>>,
    bbq_client: BbqClient,
}

enum NewFileSource {
    Oid(String),
    NewFile(Arc<XetWFileObject>),
}

/// Describes a write transaction
pub struct XetRepoWriteTransaction {
    remote_base_url: Url,
    config: XetConfig,
    branch: String,
    oldmdb: MerkleMemDB,
    oldsummaries: WholeRepoSummary,
    files: HashMap<String, NewFileSource>,
    delete_files: HashSet<String>,
    move_files: HashSet<(String, String)>,
    translator: Arc<PointerFileTranslatorV1>,
    author_name: String,
    author_email: String,
    bbq_client: BbqClient,
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
        // TODO: make a PointerFileTranslator that does not stage
        let translator = Mutex::new(Arc::new(PointerFileTranslator::from_config(&config).await?));
        Ok(XetRepo {
            remote_base_url: url,
            config,
            translator,
            bbq_client: BbqClient::new(),
        })
    }

    /// Performs a git pull to fetch the latest merkledb from the remote git repo
    async fn pull(&self) -> anyhow::Result<()> {
        let repo = git_repo::GitRepo::open(self.config.clone())?;
        eprintln!("Synchronizing with remote");
        repo.sync_remote_to_notes("origin")?;
        Ok(repo.sync_notes_to_dbs().await?)
    }

    /// Synchronizes the local MerkleDB with the remote MerkleDB
    pub async fn reload_merkledb(&self) -> anyhow::Result<()> {
        self.pull().await?;
        *(self.translator.lock().await) =
            Arc::new(PointerFileTranslator::from_config(&self.config).await?);
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
            let translator = self.translator.lock().await.clone();
            if translator.derive_blocks(&ptr_file).await.is_err() {
                self.reload_merkledb().await?;
            }
            // if I still can't derive blocks, this is a problem.
            // print an error and just return the pointer file
            let translator = self.translator.lock().await.clone();
            if translator.derive_blocks(&ptr_file).await.is_err() {
                error!("Unable to smudge file at {branch}/{filename}");
                FileContent::Bytes(body.to_vec())
            } else {
                let mini_smudger = translator
                    .make_mini_smudger(&PathBuf::default(), &ptr_file)
                    .await?;
                FileContent::Pointer((ptr_file, mini_smudger))
            }
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
        // we make a new translator
        self.reload_merkledb().await?;
        let translator = Arc::new(PointerFileTranslatorV1::from_config(&self.config).await?);
        let oldmdb = translator.mdb.lock().await.clone();
        let oldsummaries = translator.summarydb.lock().await.clone();
        Ok(XetRepoWriteTransaction {
            remote_base_url: self.remote_base_url.clone(),
            config: self.config.clone(),
            branch: branch.to_string(),
            oldmdb,
            oldsummaries,
            files: HashMap::new(),
            delete_files: HashSet::new(),
            move_files: HashSet::new(),
            translator,
            author_name,
            author_email,
            bbq_client: self.bbq_client.clone(),
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
        let newmdb = self.translator.mdb.lock().await;
        let mut diffdb = MerkleMemDB::default();
        diffdb.difference(&newmdb, &self.oldmdb);
        drop(newmdb);
        if diffdb.node_iterator().count() == 1 {
            // nothing to commit
            // the empty DB always has a size of 1
            return Ok(());
        }
        self.oldmdb = MerkleMemDB::default();
        let newmdbnote = encode_db_to_note(&self.config, diffdb).await?;
        let newmdbnote = base64::encode(newmdbnote);
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
            branch: "refs/notes/xet/merkledb".to_string(),
            commit_message: commit_message.to_string(),
            actions: vec![mdbaction],
            create_ref: true,
        };
        let mdb_commit_command = serde_json::to_string(&mdbcommand)
            .map_err(|_| anyhow!("Unexpected serialization error for {:?}", mdbcommand))?;
        perform_atomic_commit_query(self.remote_base_url.clone(), &mdb_commit_command).await?;
        Ok(())
    }
    async fn commit_summarydb(
        &mut self,
        author_name: &str,
        author_email: &str,
        commit_message: &str,
    ) -> Result<(), anyhow::Error> {
        let summarydb = self.translator.summarydb.lock().await;
        let diffsummarydb = whole_repo_summary_difference(&summarydb, &self.oldsummaries);
        if diffsummarydb.is_empty() {
            // nothing to commit
            return Ok(());
        }
        drop(summarydb);
        self.oldsummaries = WholeRepoSummary::default();
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
