#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"

remote=$(create_bare_repo)

git clone $remote repo_1

pushd repo_1
[[ $(git branch) == *"main"* ]] || git checkout -b main 
git xet init --force # Only worry about local configurations
echo "version=0" > data.dat
append_data_file data.dat 10000
git add data.dat 
git commit -a -m "Adding data."
git push --set-upstream origin main
popd


# Set up repo 2

git clone $remote repo_2
pushd repo_2

# TODO: maybe this can be automatic as part of the filter process.
assert_is_pointer_file data.dat

git xet init --force
git xet checkout

assert_files_equal data.dat ../repo_1/data.dat

popd 




