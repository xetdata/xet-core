use super::bbq_queries::*;
use super::*;
use gitxetcore::command::CliOverrides;
use gitxetcore::config::{remote_to_repo_info, ConfigGitPathOption, XetConfig};
use gitxetcore::user::XeteaAuth;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
/// The default path at which we manage the merkledbs
const GLOBAL_REPO_ROOT_PATH: &str = "repos";
/// The XetRepoManager manages a collection of all active XetRepos on disk.
/// Only 1 is needed per remote repo. We depend on git to maintain consistency
/// within.
pub struct XetRepoManager {
    config: XetConfig,
    root_path: PathBuf,
    cache: HashMap<String, Arc<XetRepo>>,
    overrides: Option<CliOverrides>,
    bbq_client: BbqClient,
}

impl XetRepoManager {
    /// Create a new repo manager with the given Xet config at the provided root path.
    /// If root path is not provided, ~/.xet/repos (GLOBAL_REPO_ROOT_PATH) is used.
    ///
    /// Note that it is intentional that config reading is deferred till repo initialization.
    /// This allows different repos to be created with different config
    /// parameters (like auth) for instance.
    pub fn new(config: Option<XetConfig>, root_path: Option<&Path>) -> anyhow::Result<Self> {
        let mut config = if let Some(config) = config {
            config
        } else if let Some(path) = root_path {
            XetConfig::new(
                None,
                None,
                ConfigGitPathOption::PathDiscover(path.to_path_buf()),
            )?
        } else {
            XetConfig::new(None, None, ConfigGitPathOption::NoPath)?
        };
        // disable staging
        config.staging_path = None;

        let root_path = if let Some(root_path) = root_path {
            root_path.to_path_buf()
        } else if let Some(env_p) = std::env::var_os("XET_REPO_CACHE") {
            let dir = env_p.to_string_lossy().to_string();
            PathBuf::from(shellexpand::tilde(&dir).to_string())
        } else {
            config.xet_home.join(GLOBAL_REPO_ROOT_PATH)
        };

        config.permission.create_dir_all(&root_path)?;

        if root_path.exists() && !root_path.is_dir() {
            return Err(anyhow!(
                "Path {root_path:?} exists but it shoud be a directory"
            ));
        }
        if !root_path.exists() {
            fs::create_dir_all(&root_path)
                .map_err(|_| anyhow!("Unable to create {root_path:?}. Path should be writable"))?;
        }

        let bbq_client =
            BbqClient::new().map_err(|_| anyhow!("Unable to create network client."))?;
        Ok(XetRepoManager {
            config,
            root_path,
            cache: HashMap::new(),
            overrides: None,
            bbq_client,
        })
    }

    // Gets the current user name
    pub fn get_inferred_username(&self, remote: &str) -> anyhow::Result<String> {
        let config = self
            .config
            .switch_repo_info(remote_to_repo_info(remote), self.overrides.clone())?;
        Ok(config.user.name.unwrap_or_default())
    }

    // sets the login username and password to use on future operations
    pub async fn override_login_config(
        &mut self,
        user_name: &str,
        user_token: &str,
        email: Option<&str>,
        host: Option<&str>,
    ) -> anyhow::Result<()> {
        let host = host.unwrap_or("xethub.com");

        let protocol = if host.contains("localhost") {
            // this is for testing sanity.
            // One does not usually get https on localhost.
            "http"
        } else {
            "https"
        };
        let auth = XeteaAuth::default();
        // attempt to authenticate against the host.
        let authcheck = auth
            .validate_xetea_auth(protocol, host, user_name, user_token)
            .await
            .map_err(|_| anyhow!("Failed to authenticate against {}", host))?;
        if !authcheck.ok {
            return Err(anyhow!("Unable to authenticate. Wrong username/password."));
        }
        let maybe_login_id = authcheck.login_id;

        let email = email.map(|x| x.to_string());
        self.overrides = Some(CliOverrides {
            user_name: Some(user_name.to_string()),
            user_token: Some(user_token.to_string()),
            user_email: email.clone(),
            user_login_id: maybe_login_id.clone(),
            ..Default::default()
        });
        // we manually apply it to config so it applies immediately
        self.config.user.name = Some(user_name.to_string());
        self.config.user.token = Some(user_token.to_string());
        self.config.user.email = email;
        self.config.user.login_id = maybe_login_id;
        Ok(())
    }

    /// Performs a file listing.
    pub async fn listdir(
        &self,
        remote: &str,
        branch: &str,
        path: &str,
    ) -> anyhow::Result<Vec<DirEntry>> {
        let config = self
            .config
            .switch_repo_info(remote_to_repo_info(remote), self.overrides.clone())?;
        let remote = config.build_authenticated_remote_url(remote);
        let url = git_remote_to_base_url(&remote)?;
        let body = self.bbq_client.perform_bbq_query(url, branch, path).await?;
        if let Ok(res) = serde_json::de::from_slice(&body) {
            Ok(res)
        } else {
            Err(anyhow!("Not a directory"))
        }
    }
    /// performs a general API query to
    /// http://[domain]/api/xet/repos/[username]/[repo]/[op]
    /// http_command is "get", "post" or "patch"
    pub async fn perform_api_query(
        &self,
        remote: &str,
        op: &str,
        http_command: &str,
        body: &str,
    ) -> anyhow::Result<Vec<u8>> {
        let config = self
            .config
            .switch_repo_info(remote_to_repo_info(remote), self.overrides.clone())?;
        let remote = config.build_authenticated_remote_url(remote);
        let url = git_remote_to_base_url(&remote)?;

        self.bbq_client
            .perform_api_query(url, op, http_command, body)
            .await
    }

    /// stats a path. This currently returns incomplete information
    /// and may not match up exactly with what listdir will provide.
    /// We need xetea to provide a stat endpoint.
    pub async fn stat(
        &self,
        remote: &str,
        branch: &str,
        mut path: &str,
    ) -> anyhow::Result<Option<DirEntry>> {
        // normalize the path dropping leading "/" then split
        if path.starts_with('/') {
            path = &path[1..];
        }
        let is_branch = path.is_empty();
        // ok... we did not implement a stat.
        // but we can just pull the blob and see what it contians
        let config = self
            .config
            .switch_repo_info(remote_to_repo_info(remote), self.overrides.clone())?;
        let remote = config.build_authenticated_remote_url(remote);
        let url = git_remote_to_base_url(&remote)?;
        let response = self
            .bbq_client
            .perform_stat_query(url, branch, path)
            .await?;

        let body = match response {
            Some(x) => x,
            None => return Ok(None),
        };
        let mut ret: DirEntry = serde_json::de::from_slice(&body)?;
        // if this is a branch, the name is empty.
        if is_branch {
            ret.name = branch.to_string();
            ret.object_type = "branch".to_string();
        }
        Ok(Some(ret))
    }
    /// we key the repo directories with a hash of the remote, user and token
    fn get_repo_key(remote: &str, user: &str, token: &str) -> String {
        // we use the blake3 hash for no real reason other than we need
        // a secure hash, and we already have this as a dependency
        let hash = blake3::hash(format!("{remote}\n{user}\n{token}").as_bytes());
        hash.to_hex().to_string()
    }
    /// Gets the repository for a given XetRepo
    /// If config is not provided, the one provided during construction
    /// of the XetRepoManager will be inherited.
    pub async fn get_repo(
        &mut self,
        config: Option<XetConfig>,
        remote: &str,
    ) -> anyhow::Result<Arc<XetRepo>> {
        let config = if let Some(config) = config {
            config
        } else {
            self.config.clone()
        };
        let key = XetRepoManager::get_repo_key(
            remote,
            &config.user.name.clone().unwrap_or_default(),
            &config.user.token.clone().unwrap_or_default(),
        );

        // if the repo already exists in the cache, just return it
        if let Some(repo) = self.cache.get(&key) {
            return Ok(repo.clone());
        }

        // see if the directory already exists
        // and if it exists, just open it instead of cloning it
        // (note that this may be a race here if multiple processes tries to
        // do this simultaneuously)
        let keypath = self.root_path.join(&key);
        let repo = if keypath.exists() {
            Arc::new(
                XetRepo::open(
                    Some(config.clone()),
                    self.overrides.clone(),
                    &keypath,
                    &self.bbq_client,
                )
                .await?,
            )
        } else {
            Arc::new(
                XetRepo::new(
                    Some(config.clone()),
                    self.overrides.clone(),
                    remote,
                    &self.root_path,
                    &key,
                    &self.bbq_client,
                )
                .await?,
            )
        };

        self.cache.insert(key, repo.clone());
        Ok(repo)
    }
}
