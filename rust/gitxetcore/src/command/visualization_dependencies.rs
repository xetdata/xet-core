// Retrieves and caches the data dependencies for all custom visualizations on a commit

use clap::Args;
use git2::Blob;
use normalize_path::NormalizePath;
use serde_json::Value;
use tracing::{info, warn};
use url::Url;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use crate::config::XetConfig;
use crate::{
    async_file_iterator::AsyncFileIterator,
    constants::GIT_MAX_PACKET_SIZE,
    data_processing::PointerFileTranslator,
    errors::{self, GitXetRepoError},
    git_integration::GitXetRepo,
};

#[derive(Args, Debug)]
pub struct VisualizationDependenciesArgs {
    /// A git commit reference to get repository size statistics.
    #[clap(default_value = "HEAD")]
    reference: String,

    /// If set, do not read the repository size statistics in git notes
    #[clap(long)]
    no_cache_read: bool,

    /// If set, only do not write the repository size statistics in git notes
    #[clap(long)]
    no_cache_write: bool,
}

pub async fn visualization_dependencies_command(
    config: XetConfig,
    args: &VisualizationDependenciesArgs,
) -> errors::Result<()> {
    let repo = GitXetRepo::open(config.clone())?;
    let gitrepo = repo.git_repo();
    let notes_ref = "refs/notes/xet/visualization-dependencies";
    let oid = gitrepo
        .read()
        .revparse_single(&args.reference)
        .map_err(|_| anyhow::anyhow!("Unable to resolve reference {}", &args.reference))?
        .id();

    // if no cache_read is false, and it is in git notes for the current commit, return that
    if let (false, Ok(possible_note)) = (
        args.no_cache_read,
        gitrepo.read().find_note(Some(notes_ref), oid),
    ) {
        let message = possible_note.message().ok_or_else(|| {
            GitXetRepoError::Other("Failed to get message from git note".to_string())
        })?;
        println!("{message}");
    } else {
        // hash map where:
        // key is a data (CSV) file path in the current tree
        // value is a list of visualization files that depend on that data file
        let mut deps = HashMap::<String, Vec<String>>::new();

        {
            // Needs to be inside this to avoid the read lock being active for the write below
            let g_repo_lg = gitrepo.read();

            let commit = g_repo_lg.find_commit(oid)?;
            // Find the current tree
            let tree = commit.tree()?;

            // temporary list of JSON files' blobs to read from,
            // to determine which data files they depend on.
            // first we have to smudge them, and we can't do this
            // inline in tree.walk since we do async stuff.
            let mut visualizations = Vec::<(String, String, Blob)>::new();

            tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
                if let Some(git2::ObjectType::Blob) = entry.kind() {
                    if let Ok(blob) = entry.to_object(&g_repo_lg).and_then(|x| x.peel_to_blob()) {
                        if let Some(name) = entry.name() {
                            if name.ends_with(".vg.json") || name.ends_with(".vl.json") {
                                // found a Vega JSON file, add it to the list
                                visualizations.push((root.to_string(), name.to_string(), blob));
                            }
                        }
                    }
                }
                git2::TreeWalkResult::Ok
            })?;

            // now smudge each visualization blob and see if it contains data references
            let translator = PointerFileTranslator::from_config(&config).await?;
            for (root, name, blob) in visualizations {
                // smudge if needed or passthrough to get contents
                let async_file = AsyncFileIterator::new(blob.content(), GIT_MAX_PACKET_SIZE);
                let mut output = Vec::<u8>::new();
                translator
                    .smudge_file(&PathBuf::new(), async_file, &mut output, true, None)
                    .await?;

                // interpret output as JSON and try to extract data URLs from it
                let value: Value = match serde_json::from_slice(&output) {
                    Ok(v) => v,
                    Err(_) => {
                        warn!("Unable to parse JSON from Vega(-Lite) file {}", name);
                        continue;
                    }
                };
                if let Some(obj) = value.as_object() {
                    if let Some(data_val) = obj.get("data") {
                        if let Some(data) = data_val.as_object() {
                            // Vega-Lite style -- expect single-data structure like {"data": {"url": "foo.json"}}
                            if let Some(url_val) = data.get("url") {
                                if let Some(url) = url_val.as_str() {
                                    // found a data URL; add the visualization to the map of deps with data URL as the key
                                    // (if it appears to be a local file path)
                                    add_visualization_dependency(&mut deps, &root, url, &name);
                                }
                            }
                        } else if let Some(data_vec) = data_val.as_array() {
                            // Vega style -- expect multiple-data structure like {"data": [{"url": "foo.json"}]}
                            for data_val in data_vec {
                                if let Some(data) = data_val.as_object() {
                                    if let Some(url_val) = data.get("url") {
                                        if let Some(url) = url_val.as_str() {
                                            // found a data URL; add the visualization to the map of deps with data URL as the key
                                            add_visualization_dependency(
                                                &mut deps, &root, url, &name,
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // cache the result in git notes
        let deps_str = serde_json::to_string(&deps)?;
        if !args.no_cache_write {
            let sig = gitrepo.read().signature()?;
            gitrepo
                .write()
                .note(&sig, &sig, Some(notes_ref), oid, &deps_str, false)?;
        }
        println!("{deps_str}");
    }
    Ok(())
}

fn add_visualization_dependency(
    deps: &mut HashMap<String, Vec<String>>,
    root: &str,
    url: &str,
    name: &str,
) {
    match Url::parse(url) {
        Ok(url) => {
            info!("Skipping valid URL {}; not a repo-relative path", url)
        }
        Err(err) => {
            match err {
                url::ParseError::RelativeUrlWithoutBase => {
                    // found a relative URL (relative path) -- interpret as relative to the visualization file within repo
                    let absolute_url = format!("{root}{url}");
                    let path = Path::new(&absolute_url).normalize();
                    deps.entry(path.to_string_lossy().to_string())
                        .or_default()
                        .push(format!("{root}{name}"));
                }
                _ => {
                    warn!("Got an unexpected error parsing URL {}: {}", url, err);
                }
            }
        }
    };
}
