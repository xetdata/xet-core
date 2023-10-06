#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"


unset XET_DISABLE_VERSION_CHECK
export _XET_VERSION_OVERRIDE="v0.0.0"
export XET_VERSION_CHECK_FILENAME=`pwd`/.xet/xet_version_info

remote=$(create_bare_repo)

out="$(git xet clone $remote repo_1 2>&1 > /dev/null)"  # Capture just stderr


[[ $out == *"new version"* ]] || die "Version message not in $out"

cd repo_1
out="$(git xet init --force 2>&1 > /dev/null)"  # Capture just stderr

[[ ! $out == *"new version"* ]] || die "Version message in $out on second run."

# Test that the bugfix critical version is detected.
rm $XET_VERSION_CHECK_FILENAME

export _XET_TEST_VERSION_CHECK_CRITICAL_PATTERN="repo-independent smudge command"

out="$(git xet init --force 2>&1 > /dev/null)"  # Capture just stderr

[[ $out == *"CRITICAL"* ]] || die "Version message not in $out"


