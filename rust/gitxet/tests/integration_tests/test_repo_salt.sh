#!/usr/bin/env bash
set -e
set -x

# Only run locally
unset XET_TESTING_REMOTE

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"
git xet install

# Set up repo 1
remote=$(create_bare_repo)

git clone $remote repo_1

pushd repo_1
[[ $(git branch) == *"main"* ]] || git checkout -b main
git xet init -m 2 --force
create_data_file data.dat 10000
git add data.dat
git commit -a -m "Adding data."
git push --set-upstream origin main
popd

# Rename remote 1
mv $remote "$remote"_1
remote_1="$remote"_1

# Set up repo 2
remote=$(create_bare_repo)
git clone $remote repo_2

pushd repo_2
[[ $(git branch) == *"main"* ]] || git checkout -b main
git xet init --force
cp ../repo_1/data.dat .
git add data.dat
git commit -a -m "Adding data."
git push --set-upstream origin main
popd

# Rename remote 2
mv $remote "$remote"_2
remote_2="$remote"_2

# Make sure two files are identical
assert_files_equal repo_1/data.dat repo_2/data.dat

# Make sure two pointer files MerkleHash are different
export XET_NO_SMUDGE=1
git clone $remote_1 repo_3
git clone $remote_2 repo_4

assert_is_pointer_file repo_3/data.dat
assert_is_pointer_file repo_4/data.dat

assert_pointer_file_size repo_3/data.dat 10000
assert_pointer_file_size repo_4/data.dat 10000

assert_files_not_equal repo_3/data.dat repo_4/data.dat