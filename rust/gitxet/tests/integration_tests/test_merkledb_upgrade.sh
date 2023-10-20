#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]:-$0}")" &>/dev/null && pwd 2>/dev/null)"
. "$SCRIPT_DIR/initialize.sh"
git xet install

remote=$(create_bare_repo)

git xet clone $remote repo_1

# make a v1 repo with data
pushd repo_1
[[ $(git branch) == *"main"* ]] || git checkout -b main
git xet init -m 1 --force

create_data_file data.dat 10000
git add data.dat
git commit -a -m "Adding data."

git push
popd

# clones another local copy
git xet clone $remote repo_2

# try to upgrade MDB in repo_1
pushd repo_1
if [[ -z $(echo YES | git xet merkledb upgrade | grep "Upgrading...Done") ]]; then
    die "upgrade not performed"
fi
rm .git/index
git add .
git commit -a -m "Check in pointer file changes after MDB upgrade."

git push
popd

# verifies the upgrade was successful
git xet clone $remote repo_3

# compare repo_2 and repo_3, data.dat should be identical
assert_files_equal repo_2/data.dat repo_3/data.dat

# compare repo_2 and repo_3 by pointer file, should differ
pushd repo_2
rm data.dat
XET_NO_SMUDGE=1 git checkout data.dat
popd
pushd repo_3
rm data.dat
XET_NO_SMUDGE=1 git checkout data.dat
popd

assert_files_not_equal repo_2/data.dat repo_3/data.dat
