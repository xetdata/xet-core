use crate::config::{remote_to_repo_info, XetConfig, PROD_XETEA_DOMAIN};
use crate::errors::{GitXetRepoError, Result};

use url::Url;

/// Env key for domain override
const XET_ENDPOINT: &str = "XET_ENDPOINT";

pub fn is_unauthenticated_repo_remote_url(ent: &str) -> bool {
    ent.starts_with("https://") || ent.starts_with("http://") || ent.starts_with("xet://")
}

pub fn authenticate_remote_url(
    url: &str,
    config: &XetConfig,
) -> Result<(String, Option<Vec<String>>)> {
    let mut remote = url.to_owned();
    let mut opt_args = None;

    if remote.starts_with("xet://") {
        (remote, opt_args) = xet_to_git_url(&remote)?;
    }

    let repo_info = remote_to_repo_info(&remote);
    let localized_config = config.switch_repo_info(repo_info, None)?;

    Ok((
        localized_config.build_authenticated_remote_url(&remote),
        opt_args,
    ))
}

/// Parse a xet url in format 'xet://[domain/?][user]/[repo][/branch/path?]'.
/// Return remote in format https://[domain]/user/repo and optional
/// '--branch' args for clone and checkout.
fn xet_to_git_url(url: &str) -> Result<(String, Option<Vec<String>>)> {
    let parsed = parse_xet_url(url)?;

    let opt_args = if parsed.branch.is_empty() {
        None
    } else {
        Some(vec!["--branch".to_owned(), parsed.branch])
    };

    Ok((parsed.remote, opt_args))
}

#[derive(Debug, PartialEq)]
pub struct XetPathInfo {
    remote: String,
    branch: String,
    path: String,
}

impl XetPathInfo {
    /// Parse a xet URL in format 'xet://[domain/?][user]/[repo][/branch/path?]' and return
    /// XetPathInfo.
    /// [domain] is 'xethub.com' by default.
    /// The logic is mostly borrowed from pyxet.
    pub fn parse(url: &str, force_domain: &str) -> Result<Self> {
        let url = url.strip_prefix('/').unwrap_or(url);

        let mut parse =
            Url::parse(url).map_err(|e| GitXetRepoError::InvalidRemote(e.to_string()))?;
        if parse.scheme() == "" {
            parse.set_scheme("xet").map_err(|_| {
                GitXetRepoError::InvalidRemote("Failed to reset scheme to xet".to_owned())
            })?;
        }

        let mut domain = force_domain.to_owned();
        // support force_domain with a scheme (http/https)
        let domain_split: Vec<_> = domain.split("://").collect();
        let mut scheme = "https".to_owned();
        if domain_split.len() == 2 {
            scheme = domain_split[0].to_owned();
            domain = domain_split[1].to_owned();
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
                } else {
                    // this is of the for xet://user/repo/...
                    // join user back with path
                    let newpath = format!("{}{}", host_str, parse.path());
                    // replace the host
                    parse.set_host(Some(&domain)).map_err(|_| {
                        GitXetRepoError::InvalidRemote(format!("Invalid domain {domain}"))
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

        return Ok(XetPathInfo {
            remote: format!(
                "{scheme}://{}{replacement_parse_path}",
                parse.host().unwrap().to_string()
            ),
            branch: branch,
            path: path,
        });
    }
}

fn parse_xet_url(url: &str) -> Result<XetPathInfo> {
    // Get domain override
    let domain_override = std::env::var(XET_ENDPOINT).unwrap_or(PROD_XETEA_DOMAIN.to_owned());

    XetPathInfo::parse(url, &domain_override)
}

pub mod test_routines {
    use super::{parse_xet_url, XetPathInfo};
    use crate::errors::Result;

    pub fn assert_xet_url_parse_result(xeturl: &str, expected: &XetPathInfo) -> Result<()> {
        let parsed = parse_xet_url(xeturl)?;

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
        let parsed = parse_xet_url(xeturl);

        assert!(parsed.is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::{
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
                remote: "https://xethub.com/user/repo".to_owned(),
                branch: "branch".to_owned(),
                path: "hello/world".to_owned(),
            },
        )?;

        assert_xet_url_parse_result(
            "xet://xethub.com/user/repo/branch/hello/world/",
            &XetPathInfo {
                remote: "https://xethub.com/user/repo".to_owned(),
                branch: "branch".to_owned(),
                path: "hello/world/".to_owned(),
            },
        )?;

        assert_xet_url_parse_result(
            "xet://xethub.com/user/repo/branch/",
            &XetPathInfo {
                remote: "https://xethub.com/user/repo".to_owned(),
                branch: "branch".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_parse_result(
            "xet://xethub.com/user/repo/branch",
            &XetPathInfo {
                remote: "https://xethub.com/user/repo".to_owned(),
                branch: "branch".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_parse_result(
            "xet://xethub.com/user/repo",
            &XetPathInfo {
                remote: "https://xethub.com/user/repo".to_owned(),
                branch: "".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_with_domain_override_parse_result(
            "xet://xethub.com/user/repo/branch",
            "xetbeta.com",
            &XetPathInfo {
                remote: "https://xetbeta.com/user/repo".to_owned(),
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
                remote: "https://xethub.com/user/repo".to_owned(),
                branch: "branch".to_owned(),
                path: "hello/world".to_owned(),
            },
        )?;

        assert_xet_url_parse_result(
            "xet://user/repo/branch/hello/world/",
            &XetPathInfo {
                remote: "https://xethub.com/user/repo".to_owned(),
                branch: "branch".to_owned(),
                path: "hello/world/".to_owned(),
            },
        )?;

        assert_xet_url_parse_result(
            "xet://user/repo/branch/",
            &XetPathInfo {
                remote: "https://xethub.com/user/repo".to_owned(),
                branch: "branch".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_parse_result(
            "xet://user/repo/branch",
            &XetPathInfo {
                remote: "https://xethub.com/user/repo".to_owned(),
                branch: "branch".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_parse_result(
            "xet://user/repo",
            &XetPathInfo {
                remote: "https://xethub.com/user/repo".to_owned(),
                branch: "".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_with_domain_override_parse_result(
            "xet://user/repo/branch",
            "xetbeta.com",
            &XetPathInfo {
                remote: "https://xetbeta.com/user/repo".to_owned(),
                branch: "branch".to_owned(),
                path: "".to_owned(),
            },
        )?;

        assert_xet_url_parse_err("xet://user");

        Ok(())
    }
}
