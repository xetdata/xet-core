#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"

if [[ ! -z $(ping -c 1 github.com 2>&1 | grep -i "unknown host") ]] ; then 
  echo "github.com unreachable, skipping test."
  exit 0
fi


unset XET_DISABLE_VERSION_CHECK
export _XET_TEST_VERSION_OVERRIDE="v0.0.0"
export XET_UPGRADE_CHECK_FILENAME=`pwd`/.xet/xet_version_info

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


