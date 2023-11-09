#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"
setup_basic_run_environment

remote=$(create_bare_repo)

if git lfs version ; then 
  echo "LFS is available."
else 
  2>&1 echo "WARNING: git lfs not installed; skipping."
  exit 0
fi

git clone $remote repo_1
cd repo_1

[[ ! -e ./.git/hooks/post-merge ]] || die "hook already installed" 
[[ ! -e ./.git/hooks/post-commit ]] || die "hook already installed" 
[[ ! -e ./.git/hooks/post-checkout ]] || die "hook already installed" 

git xet init --enable-locking --force

[[ -e ./.git/hooks/post-merge ]] || die "hook not installed" 
[[ -e ./.git/hooks/post-commit ]] || die "hook not installed" 
[[ -e ./.git/hooks/post-checkout ]] || die "hook not installed" 
