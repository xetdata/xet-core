#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"



# Do the global config
git xet install

[[ ! -z $(git config --global --get-regex 'filter.xet.*') ]] || die "git config not set."

git xet uninstall 

[[ -z $(git config --global --get-regex 'filter.xet.*') ]] || die "git config not purged."


# Look at the local config

mkdir repo
cd repo
git init

git xet install --local

[[ ! -z $(git config --local --get-regex 'filter.xet.*') ]] || die "git config not set."

git xet uninstall --local

[[ -z $(git config --local --get-regex 'filter.xet.*') ]] || die "git config not purged."
