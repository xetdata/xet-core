#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"

remote=$(create_bare_repo)

git clone $remote repo_1

#export GIT_CONFIG=$GIT_CONFIG_GLOBAL

pushd repo_1
[[ $(git branch) == *"main"* ]] || git checkout -b main
git xet install # This should write to the global config file.

# Test to make sure the global config actually is changed.
[[ $(git config --get --global filter.xet.process) == "git xet filter" ]] \
  || die "Global config for filter.xet.process not set."

git xet init --force  # This should write to the local repo without changed the global config 

[[ -z  $(git config --get --local filter.xet.process) ]] \
  || die "Local config for filter.xet.process set when global is present."

git xet init --force --force-local-config # This should write to the local config

[[ $(git config --get --local filter.xet.process) == "git xet filter" ]] \
  || die "Local config for filter.xet.process not set."

create_data_file data.dat 10000
git add data.dat 
git commit -a -m "Adding data."
git push origin main
popd


# Set up repo 2
git clone $remote repo_2
pushd repo_2

# Now, everything should have been properly configured automatically
assert_files_equal data.dat ../repo_1/data.dat






