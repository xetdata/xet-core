#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"

unset XET_DISABLE_VERSION_CHECK
export _XET_VERSION_OVERRIDE="v0.0.0"

remote=$(create_bare_repo)

out="$(git xet clone $remote repo_1 2>&1 > /dev/null)"  # Capture just stderr

[[ $out == *"new version"* ]] || die "Version message not in $out"
