use crate::config::{remote_to_repo_info, XetConfig, PROD_XETEA_DOMAIN};
use crate::errors::{GitXetRepoError, Result};

use tracing::{debug, info};
use url::Url;

/// Env key for domain override
const XET_ENDPOINT: &str = "XET_ENDPOINT";

pub fn repo_name_from_remote(remote: &str) -> Option<String> {
    let last = remote.split('/').last().to_owned();
    last.map(|f| {
        if let Some(stripped) = f.strip_suffix(".git") {
            stripped
        } else {
            f
        }
    })
    .map(|f| f.into())
}

fn is_unauthenticated_repo_remote_url(ent: &str) -> bool {
    ent.starts_with("https://") || ent.starts_with("http://") || ent.starts_with("xet://")
}

pub fn is_remote_url(ent: &str) -> bool {
    ent.starts_with("https://")
        || ent.starts_with("http://")
        || ent.starts_with("xet://")
        || ent.starts_with("ssh://")
}

/// Parse a url and return the remote url, repo name and a branch
/// field if the url contains a branch field.
pub fn parse_remote_url(url: &str) -> Result<(String, String, Option<String>)> {
    if url.starts_with("xet://") {
        let parsed = parse_xet_url(url)?;

        let branch = if parsed.branch.is_empty() {
            None
        } else {
            Some(parsed.branch)
        };

        Ok((parsed.remote_url, parsed.repo, branch))
    } else {
        Ok((
            url.to_owned(),
            repo_name_from_remote(url)
                .ok_or_else(|| GitXetRepoError::InvalidRemote(url.to_owned()))?,
            None,
        ))
    }
}

/// Build an authenticated remote url if not authenticated.
pub fn authenticate_remote_url(remote: &str, config: &XetConfig) -> Result<String> {
    if is_unauthenticated_repo_remote_url(remote) {
        let repo_info = remote_to_repo_info(remote);
        let localized_config = config.switch_repo_info(repo_info, None)?;
        Ok(localized_config.build_authenticated_remote_url(remote))
    } else {
        Ok(remote.to_owned())
    }
}

#[derive(Debug, PartialEq)]
pub struct XetPathInfo {
    pub remote_url: String,
    pub repo: String,
    pub branch: String,
    pub path: String,
}

impl XetPathInfo {
    /// Parse a xet URL in format 'xet://[domain/?][user]/[repo][/branch/path?]' and return
    /// XetPathInfo.
    /// [domain] is 'xethub.com' by default.
    /// The logic is mostly borrowed from pyxet.
    fn parse(url: &str, force_domain: &str) -> Result<Self> {
        debug!("Parsing URL '{url}', force_domain = '{force_domain}'");
        let url = url.strip_suffix('/').unwrap_or(url);

        let mut parse =
            Url::parse(url).map_err(|e| GitXetRepoError::InvalidRemote(e.to_string()))?;
        if parse.scheme() == "" {
            parse.set_scheme("xet").map_err(|_| {
                GitXetRepoError::InvalidRemote("Failed to reset scheme to xet".to_owned())
            })?;
        }

        // closure to split out domain and port, returns Err if port is an invalid u16.
        let split_out_port = |domain: &str| -> Result<(String, Option<u16>)> {
            let host_port_split = domain.split(':').collect::<Vec<_>>();
            if host_port_split.len() == 2 {
                Ok((
                    host_port_split[0].to_string(),
                    Some(host_port_split[1].parse::<u16>().map_err(|_| {
                        GitXetRepoError::InvalidRemote(format!(
                            "Invalid port {}",
                            host_port_split[1]
                        ))
                    })?),
                ))
            } else {
                Ok((domain.to_string(), None))
            }
        };

        // support force_domain with a scheme (http/https)
        let domain_split: Vec<_> = force_domain.split("://").collect::<Vec<_>>();
        let mut scheme = "https".to_owned();
        let port: Option<u16>;
        let domain;
        if domain_split.len() == 2 {
            scheme = domain_split[0].to_owned();
            (domain, port) = split_out_port(domain_split[1])?;
        } else {
            (domain, port) = split_out_port(force_domain)?;
        }

        if parse.scheme() != "xet" {
            return Err(GitXetRepoError::InvalidRemote(
                "Invalid protocol".to_owned(),
            ));
        }

        // Handle the case where we are xet://user/repo. In which case the domain
        // parsed is not xethub.com and domain="user".
        // we rewrite the parse the handle this case early.
        if let Some(host) = parse.host() {
            let host_str = host.to_string();
            if host_str != domain {
                if host_str == "xethub.com" {
                    parse.set_host(Some(&domain)).map_err(|_| {
                        GitXetRepoError::InvalidRemote(format!("Invalid domain {domain}"))
                    })?;
                    parse.set_port(port).map_err(|_| {
                        GitXetRepoError::InvalidRemote(format!("Invalid port {:?}", port))
                    })?;
                } else {
                    // this is of the for xet://user/repo/...
                    // join user back with path
                    let newpath = format!("{}{}", host_str, parse.path());
                    // replace the host
                    parse.set_host(Some(&domain)).map_err(|_| {
                        GitXetRepoError::InvalidRemote(format!("Invalid domain {domain}"))
                    })?;
                    parse.set_port(port).map_err(|_| {
                        GitXetRepoError::InvalidRemote(format!("Invalid port {:?}", port))
                    })?;
                    parse.set_path(&newpath);
                }
            }
        }

        // Split the known path and try to split out the user/repo/branch/path components
        let path = parse.path();
        let components: Vec<_> = path.split('/').collect();
        // path always begin with a '/', so 1st component is always empty
        // so the minimum for a remote is xethub.com/user/repo
        if components.len() < 3 {
            return Err(GitXetRepoError::InvalidRemote(
                "Invalid Xet URL format: Expecting xet://user/repo/[branch]/[path]".to_owned(),
            ));
        }

        let repo = components[2].to_owned();

        let branch = if components.len() >= 4 {
            components[3].to_owned()
        } else {
            "".to_owned()
        };

        let path = if components.len() >= 5 {
            components[4..].join("/")
        } else {
            "".to_owned()
        };

        // we leave url with the first 3 components. i.e. "/user/repo"
        let replacement_parse_path = components[..3].join("/");

        let port_string;
        if let Some(parsed_port) = parse.port() {
            port_string = format!(":{}", parsed_port)
        } else {
            port_string = "".to_owned()
        };

        let ret = XetPathInfo {
            remote_url: format!(
                "{scheme}://{}{}{replacement_parse_path}",
                parse.host().unwrap(),
                port_string,
            ),
            branch,
            path,
            repo,
        };

        info!("Parsed URL '{url}', (force_domain='{force_domain}') to {ret:?}.");

        Ok(ret)
    }
}

pub fn parse_xet_url(url: &str) -> Result<XetPathInfo> {
    // Get domain override
    let domain_override = std::env::var(XET_ENDPOINT).unwrap_or(PROD_XETEA_DOMAIN.to_owned());

    XetPathInfo::parse(url, &domain_override)
}

pub mod test_routines {
    use super::XetPathInfo;
    use crate::{config::PROD_XETEA_DOMAIN, errors::Result};

    pub fn assert_xet_url_parse_result(xeturl: &str, expected: &XetPathInfo) -> Result<()> {
        let parsed = XetPathInfo::parse(xeturl, PROD_XETEA_DOMAIN)?;

        assert_eq!(&parsed, expected);

        Ok(())
    }

    pub fn assert_xet_url_with_domain_override_parse_result(
        xeturl: &str,
        domain_override: &str,
        expected: &XetPathInfo,
    ) -> Result<()> {
        let parsed = XetPathInfo::parse(xeturl, domain_override)?;

        assert_eq!(&parsed, expected);

        Ok(())
    }

    pub fn assert_xet_url_parse_err(xeturl: &str) {
        let parsed = XetPathInfo::parse(xeturl, PROD_XETEA_DOMAIN);

        assert!(parsed.is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::{
        repo_name_from_remote,
        test_routines::{
            assert_xet_url_parse_err, assert_xet_url_parse_result,
            assert_xet_url_with_domain_override_parse_result,
        },
        XetPathInfo,
    };
    use crate::errors::Result;

    #[test]
    fn test_parse_xet_url() -> Result<()> {
        assert_xet_url_parse_result(
            "xet://xethub.com/user/repo/branch/hello/world",
            &XetPathInfo {
                remote_url: "https://xethub.com/user/repo".to_owned(),
                repo: "repo".to_owned(),
                branch: "branch".to_owned(),
                path: "hello/world".to_owned(),
            },
        )?;

        assert_xet_url_parse_result(
            "xet://xethub.com/user/repo/branch/hello/world/",
            &XetPathInfo {
                remote_url: "https://xethub.com/user/repo".to_owned(),
                repo: "repo".to_owned(),
                branch: "branch".to_owned(),
                path: "hello/world/".to_owned(),
            },
        )?;

        assert_xet_url_parse_result(
            "xet://xethub.com/user/repo/branch/",
            &XetPathInfo {
                remote_url: "https://xethub.com/user/repo".to_owned(),
                repo: "repo".to_owned(),
                branch: "branch".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_parse_result(
            "xet://xethub.com/user/repo/branch",
            &XetPathInfo {
                remote_url: "https://xethub.com/user/repo".to_owned(),
                repo: "repo".to_owned(),
                branch: "branch".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_parse_result(
            "xet://xethub.com/user/repo",
            &XetPathInfo {
                remote_url: "https://xethub.com/user/repo".to_owned(),
                repo: "repo".to_owned(),
                branch: "".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_with_domain_override_parse_result(
            "xet://xethub.com/user/repo/branch",
            "xetbeta.com",
            &XetPathInfo {
                remote_url: "https://xetbeta.com/user/repo".to_owned(),
                repo: "repo".to_owned(),
                branch: "branch".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_with_domain_override_parse_result(
            "xet://xethub.com/user/repo/branch",
            "https://localhost:1234",
            &XetPathInfo {
                remote_url: "https://localhost:1234/user/repo".to_owned(),
                repo: "repo".to_owned(),
                branch: "branch".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_with_domain_override_parse_result(
            "xet://xethub.com/user/repo/branch",
            "http://localhost:1234",
            &XetPathInfo {
                remote_url: "http://localhost:1234/user/repo".to_owned(),
                repo: "repo".to_owned(),
                branch: "branch".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_parse_err("xet://xethub.com/user");

        Ok(())
    }

    #[test]
    fn test_parse_xet_url_truncated() -> Result<()> {
        assert_xet_url_parse_result(
            "xet://user/repo/branch/hello/world",
            &XetPathInfo {
                remote_url: "https://xethub.com/user/repo".to_owned(),
                repo: "repo".to_owned(),
                branch: "branch".to_owned(),
                path: "hello/world".to_owned(),
            },
        )?;

        assert_xet_url_parse_result(
            "xet://user/repo/branch/hello/world/",
            &XetPathInfo {
                remote_url: "https://xethub.com/user/repo".to_owned(),
                repo: "repo".to_owned(),
                branch: "branch".to_owned(),
                path: "hello/world/".to_owned(),
            },
        )?;

        assert_xet_url_parse_result(
            "xet://user/repo/branch/",
            &XetPathInfo {
                remote_url: "https://xethub.com/user/repo".to_owned(),
                repo: "repo".to_owned(),
                branch: "branch".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_parse_result(
            "xet://user/repo/branch",
            &XetPathInfo {
                remote_url: "https://xethub.com/user/repo".to_owned(),
                repo: "repo".to_owned(),
                branch: "branch".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_parse_result(
            "xet://user/repo",
            &XetPathInfo {
                remote_url: "https://xethub.com/user/repo".to_owned(),
                repo: "repo".to_owned(),
                branch: "".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_with_domain_override_parse_result(
            "xet://user/repo/branch",
            "xetbeta.com",
            &XetPathInfo {
                remote_url: "https://xetbeta.com/user/repo".to_owned(),
                repo: "repo".to_owned(),
                branch: "branch".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_with_domain_override_parse_result(
            "xet://user/repo/branch",
            "http://localhost:3000",
            &XetPathInfo {
                remote_url: "http://localhost:3000/user/repo".to_owned(),
                repo: "repo".to_owned(),
                branch: "branch".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_with_domain_override_parse_result(
            "xet://user/repo/branch",
            "xetbeta.com:5500",
            &XetPathInfo {
                remote_url: "https://xetbeta.com:5500/user/repo".to_owned(),
                repo: "repo".to_owned(),
                branch: "branch".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_parse_err("xet://user");

        Ok(())
    }

    #[test]
    fn test_repo_name_from_remote() {
        assert_eq!(
            repo_name_from_remote("https://xethub.com/user/blah.git")
                .unwrap()
                .as_str(),
            "blah"
        );
        assert_eq!(
            repo_name_from_remote("xet@xethub.com:user/bloof.git")
                .unwrap()
                .as_str(),
            "bloof"
        );
    }
}
