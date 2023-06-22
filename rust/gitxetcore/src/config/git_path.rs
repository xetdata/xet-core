use crate::config::env;
use crate::config::env::XetEnv;
use crate::config::util::OptionHelpers;
use crate::config::ConfigError;
use crate::config::ConfigError::RepoPathNotExistingDir;
use crate::git_integration::git_repo::GitRepo;
use crate::git_integration::git_wrap;
use std::path::PathBuf;
use tracing::info;

/// Used in XetConfig::new
pub enum ConfigGitPathOption {
    /// Provided directory may be part of a git repo. attempts to discover the root
    /// if available. If not in a git repo, behaves as if NoPath is provided
    PathDiscover(PathBuf),
    /// Use the current directory which may be part of a git repo. attempts to
    /// discover the root. If not in a git repo, behaves as if NoPath is provided
    CurdirDiscover,
    /// Do not associate with a path
    NoPath,
}

/// Converts a single known remote to a repo info
pub fn remote_to_repo_info(remote: &str) -> RepoInfo {
    RepoInfo {
        env: XetEnv::from_xetea_url(remote),
        maybe_git_path: None,
        remote_urls: vec![remote.to_string()],
    }
}

impl ConfigGitPathOption {
    /// Resolve the git path into a [RepoInfo] containing the resolved repo location (if any),
    /// the remote urls of the repo, and the Xetea Env being used.
    pub fn into_repo_info(self) -> Result<RepoInfo, ConfigError> {
        let maybe_git_path = self.into_git_path().validate(|git_path| {
            if !git_path.exists() || !git_path.is_dir() {
                Err(RepoPathNotExistingDir(git_path.to_path_buf()))
            } else {
                Ok(())
            }
        })?;

        let remote_urls = if let Some(ref p) = maybe_git_path {
            GitRepo::get_remote_urls(Some(p)).unwrap_or_else(|_| vec![])
        } else {
            vec![]
        };
        // check the xetea_envs
        let xetea_envs = remote_urls
            .iter()
            .map(|url| XetEnv::from_xetea_url(url))
            .collect::<Vec<_>>();
        let env = env::resolve_env(xetea_envs)?;
        info!("Resolved Xet Env: {env:?}");

        Ok(RepoInfo {
            env,
            maybe_git_path,
            remote_urls,
        })
    }

    /// Get the git path indicated by self. If no .git dir can be found, then [None] is
    /// returned.
    fn into_git_path(self) -> Option<PathBuf> {
        match self {
            ConfigGitPathOption::PathDiscover(p) => git_wrap::get_git_path(Some(p)).unwrap_or(None),
            ConfigGitPathOption::CurdirDiscover => git_wrap::get_git_path(None).unwrap_or(None),
            ConfigGitPathOption::NoPath => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RepoInfo {
    pub env: XetEnv,
    pub maybe_git_path: Option<PathBuf>,
    pub remote_urls: Vec<String>,
}

impl Default for RepoInfo {
    fn default() -> Self {
        Self {
            env: XetEnv::Prod,
            maybe_git_path: None,
            remote_urls: vec![],
        }
    }
}

#[cfg(test)]
mod test_git_config_path {
    use std::env;
    use std::path::PathBuf;
    use std::str::FromStr;

    use crate::config::env::XetEnv;
    use crate::config::git_path::ConfigGitPathOption;

    use crate::git_integration::git_repo::test_tools::TestRepoPath;
    use crate::git_integration::git_wrap;

    #[test]
    fn test_discover_into_git_path() {
        let tmp_repo = TestRepoPath::new("git-path-discover").unwrap();
        let path = tmp_repo.path;
        let expected_path = path.join(".git");
        git_wrap::run_git_captured(Some(&path), "init", &[], true, None).unwrap();
        let cfg_option = ConfigGitPathOption::PathDiscover(path);
        let git_path = cfg_option.into_git_path().unwrap();
        assert_eq!(expected_path, git_path);
    }

    #[test]
    fn test_discover_into_git_path_not_found() {
        let path = PathBuf::from_str("/").unwrap();
        let cfg_option = ConfigGitPathOption::PathDiscover(path);
        assert!(cfg_option.into_git_path().is_none());
    }

    #[test]
    #[ignore] // this writes the `env::current_dir`, which other tests depend on. This can cause occasional errors.
    fn test_curdir_into_git_path() {
        let tmp_repo = TestRepoPath::new("git-path-discover").unwrap();
        let path = tmp_repo.path;
        git_wrap::run_git_captured(Some(&path), "init", &[], true, None).unwrap();
        let expected_path = path.join(".git").canonicalize().unwrap(); // need canonical path since MacOS symlinks /var -> /private/var

        let old_pwd = env::current_dir().unwrap();
        env::set_current_dir(&path).unwrap();

        let cfg_option = ConfigGitPathOption::CurdirDiscover;
        let git_path = cfg_option.into_git_path().unwrap();
        assert_eq!(expected_path, git_path);

        env::set_current_dir(old_pwd).unwrap();
    }

    #[test]
    fn test_nopath_into_git_path() {
        let cfg_option = ConfigGitPathOption::NoPath;
        assert!(cfg_option.into_git_path().is_none());
    }

    // Note: Will panic if error encountered
    // Note: CircleCI has an override for https://github.com -> ssh://git@github.com in its
    // config, meaning that either you should use an ssh url or not use github.com when testing
    // remote urls
    fn add_remote_repo(path: &PathBuf, name: &str, url: &str) {
        git_wrap::run_git_captured(Some(path), "remote", &["add", name, url], true, None).unwrap();
    }

    #[test]
    fn test_into_repo_info() {
        let tmp_repo = TestRepoPath::new("into_repo_info").unwrap();
        let path = tmp_repo.path;
        git_wrap::run_git_captured(Some(&path), "init", &[], true, None).unwrap();
        let expected_path = path.join(".git");
        // Add a remote repo
        let repo_url = "https://xethub.com/org/repo";
        add_remote_repo(&path, "origin", repo_url);
        // resolve to repo info
        let cfg_option = ConfigGitPathOption::PathDiscover(path);
        let repo_info = cfg_option.into_repo_info().unwrap();
        assert_eq!(XetEnv::Prod, repo_info.env);
        assert_eq!(vec![repo_url], repo_info.remote_urls);
        assert_eq!(expected_path, repo_info.maybe_git_path.unwrap());
    }

    #[test]
    fn test_into_repo_info_multiple_remotes() {
        let tmp_repo = TestRepoPath::new("into_repo_info_multi").unwrap();
        let path = tmp_repo.path;
        git_wrap::run_git_captured(Some(&path), "init", &[], true, None).unwrap();
        let expected_path = path.join(".git");
        // Add a couple remote repos
        let origin_url = "https://xethub.com/org/repo";
        add_remote_repo(&path, "origin", origin_url);
        let github_url = "ssh://git@github.com/org/repo";
        add_remote_repo(&path, "github", github_url);
        // resolve to repo info
        let cfg_option = ConfigGitPathOption::PathDiscover(path);
        let repo_info = cfg_option.into_repo_info().unwrap();
        assert_eq!(XetEnv::Prod, repo_info.env);
        assert_eq!(vec![github_url, origin_url], repo_info.remote_urls);
        assert_eq!(expected_path, repo_info.maybe_git_path.unwrap());
    }

    #[test]
    fn test_into_repo_info_multiple_remotes_same_env() {
        let tmp_repo = TestRepoPath::new("into_repo_info_multi_same").unwrap();
        let path = tmp_repo.path;
        git_wrap::run_git_captured(Some(&path), "init", &[], true, None).unwrap();
        let expected_path = path.join(".git");
        // Add a couple remote repos
        let origin_url = "https://xethub.com/org/repo";
        add_remote_repo(&path, "origin", origin_url);
        let other_url = "https://xethub.com/mine/repo";
        add_remote_repo(&path, "other", other_url);
        // resolve to repo info
        let cfg_option = ConfigGitPathOption::PathDiscover(path);
        let repo_info = cfg_option.into_repo_info().unwrap();
        assert_eq!(XetEnv::Prod, repo_info.env);
        assert_eq!(vec![origin_url, other_url], repo_info.remote_urls);
        assert_eq!(expected_path, repo_info.maybe_git_path.unwrap());
    }

    #[test]
    fn test_into_repo_info_no_repo() {
        let cfg_option = ConfigGitPathOption::NoPath;
        let repo_info = cfg_option.into_repo_info().unwrap();
        assert_eq!(XetEnv::Prod, repo_info.env);
        assert!(repo_info.remote_urls.is_empty());
        assert!(repo_info.maybe_git_path.is_none());
    }

    /*

    #[test]
    fn test_into_repo_info_failed_remotes() {
        let tmp_repo = TestRepoPath::new("into_repo_info_fail_remote").unwrap();
        let path = tmp_repo.path;
        git_wrap::run_git_captured(Some(&path), "init", &[], true, None).unwrap();
        // Add a couple remote repos that conflict
        let origin_url = "https://xethub.com/org/repo";
        add_remote_repo(&path, "origin", origin_url);
        let beta_url = "https://xethub1.com/org/repo";
        add_remote_repo(&path, "beta", beta_url);
        // resolve to repo info
        let cfg_option = ConfigGitPathOption::PathDiscover(path);
        assert_err!(cfg_option.into_repo_info());
    }
    */
}
