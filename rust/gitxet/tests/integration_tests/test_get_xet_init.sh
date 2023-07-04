#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"
git xet install

remote=$(create_bare_repo)

git clone $remote repo_1

pushd repo_1

[[ $(git branch) == *"main"* ]] || git checkout -b main
git xet init -m 1 --force
# check version is 1
[[ ! -z $(git xet merkledb version | grep "1") ]] || die "merkledb version is not 1"
[[ -e ./.git/refs/notes/xet/merkledb ]] || die "merkledb v1 ref notes not set"

# upgrade to v2 is Ok when v1 is empty
git xet init -m 2 --force
[[ ! -z $(git xet merkledb version | grep "2") ]] || die "merkledb version is not 2"
[[ -e ./.git/refs/notes/xet/merkledbv2 ]] || die "merkledb v2 ref notes not set"

# downgrade should report error
if [[ -z $(git xet init -m 1 --force 2>&1 | grep "illegal") ]]; then
    die "git-xet didn't block illegal action"
fi

# running init on the same version should succeed
git xet init -m 2 --force
[[ ! -z $(git xet merkledb version | grep "2") ]] || die "merkledb version is not 2"

create_data_file data.dat 10000
git add data.dat
git commit -a -m "Adding data."

# init on the same version with repo not empty should succeed
git xet init -m 2 --force
[[ ! -z $(git xet merkledb version | grep "2") ]] || die "merkledb version is not 2"

popd

git clone $remote repo_2
pushd repo_2

git xet init -m 1 --force

create_data_file data.dat 10000
git add data.dat
git commit -a -m "Adding data."

# check version is 1
[[ ! -z $(git xet merkledb version | grep "1") ]] || die "merkledb version is not 1"
[[ -e ./.git/refs/notes/xet/merkledb ]] || die "merkledb v1 ref notes not set"

# upgrade with repo not empty should fail (check lobal db)
if [[ -z $(git xet init -m 2 --force 2>&1 | grep "Merkle DB is not empty") ]]; then
    die "git-xet didn't block illegal action"
fi

git push origin main

popd

git clone $remote repo_3
pushd repo_3

rm .git/xet/merkledb.db

# check version is 1
[[ ! -z $(git xet merkledb version | grep "1") ]] || die "merkledb version is not 1"

# upgrade when repo not empty should fail (check db in notes)
if [[ -z $(git xet init -m 2 --force 2>&1 | grep "Merkle DB is not empty") ]]; then
    die "git-xet didn't block illegal action"
fi

popd