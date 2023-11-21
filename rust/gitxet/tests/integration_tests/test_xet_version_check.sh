#!/usr/bin/env bash
set -e
set -x

global_tmp_dir=/tmp

if [[ ! -z ${TEMP} ]] ; then
  global_tmp_dir="$TEMP"
elif [[ ! -z ${TEMP_DIR} ]] ; then 
  global_tmp_dir="$TEMP_DIR"
elif [[ ! -z ${TMPDIR} ]] ; then
  global_tmp_dir="$TMPDIR"
fi

# Set this here, before HOME gets reset to isolate git.
xet_version_info_file_full="${global_tmp_dir}/git_xet_version_check_api_cache_full"
xet_version_info_file_latest="${global_tmp_dir}/git_xet_version_check_api_cache_latest"

github_api_url_full="https://api.github.com/repos/xetdata/xet-tools/releases"
github_api_url_latest="https://api.github.com/repos/xetdata/xet-tools/releases/latest"

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"
setup_basic_run_environment

download_new_version_file () { 
  local version_url="$1"
  local version_file="$2"
  curl -L "$version_url" > "$version_file.temp"

  if [[ ! -z $(grep "API rate limit exceeded" "$version_file.temp") ]] ; then
    >&2 echo "Skipping update of cache of local information; API rate limit exceded."
    rm -f "$version_file.temp"
  elif [[ -z $(grep -l "github.com/xetdata/xet-tools/releases" "$version_file.temp") ]] ; then 
    >&2 echo "Skipping update of cache of local information; downloaded file does not contain the correct info." 
    rm -f "$version_file.temp"
  else 
    mv "$version_file.temp" "$version_file" 
  fi
}

refresh_version_file() {
  local version_url="$1"
  local version_file="$2"

# To avoid unneeded pings of the github API, without causing tests to fail, attempt 
if [[ -e "$file" ]] ; then 
  # If it has been written in the last 24 hours, just use that.  Otherwise, download a new version.
  if [[ ! -z $(find "$file" -mtime +1d -print) ]] ; then
    download_new_version_file "$version_url" "$version_file"
  fi
else 
    download_new_version_file "$version_url" "$version_file"
fi

}

refresh_version_file $github_api_url_full $xet_version_info_file_full
refresh_version_file $github_api_url_latest $xet_version_info_file_latest

if [[ ! -e "$xet_version_info_file_full" ]] ||  [[ ! -e "$xet_version_info_file_latest" ]]; then
  2<&1 echo "Error downloading version info information; skipping test."
  exit 0
fi

unset XET_DISABLE_VERSION_CHECK
export _XET_TEST_VERSION_OVERRIDE="v0.0.0"
export XET_UPGRADE_CHECK_FILENAME=`pwd`/.xet/xet_version_info
export _XET_VERSION_QUERY_URL_OVERRIDE_FULL="file://$xet_version_info_file_full"
export _XET_VERSION_QUERY_URL_OVERRIDE_LATEST="file://$xet_version_info_file_latest"

env

remote=$(create_bare_repo)

out="$(git xet clone $remote repo_1 2>&1 > /dev/null)"  # Capture just stderr

if [[ ! $out == *"new version"* ]] ; then 
  # See if the log directory contains a rate limit message, which is one way this can fail 
  if [[ ! -z $(grep "API rate limit exceeded" logs/*) ]] ; then 
     echo "WARNING: API rate limit exceeded for github version query; skipping tests."
     exit 0
  fi
     
  die "Version message not in $out."
fi

cd repo_1
out_2="$(git xet init --force 2>&1 > /dev/null)"  # Capture just stderr

[[ ! $out_2 == *"new version"* ]] || die "Version message in \{\{\{ $out_2 \}\}\} on second run."

# Test that the bugfix critical version is detected.
rm -f $XET_UPGRADE_CHECK_FILENAME

export _XET_TEST_VERSION_CHECK_CRITICAL_PATTERN="repo-independent smudge command"

out="$(git xet init --force 2>&1 > /dev/null)"  # Capture just stderr

[[ $out == *"CRITICAL"* ]] || die "Critical version message not in $out"


