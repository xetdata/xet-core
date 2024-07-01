#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]:-$0}")" &>/dev/null && pwd 2>/dev/null)"
. "$SCRIPT_DIR/initialize.sh"

setup_xetldfs_testing_env 

git xet install

remote=$(create_bare_repo)

git clone $remote repo_1

# file larger than 100 bytes will be checked-in as pointer file
export XET_CAS_SIZETHRESHOLD=16
text_1="abcdefghijklmnopqrstuvwxyz"

pushd repo_1
[[ $(git branch) == *"main"* ]] || git checkout -b main
git xet init --force
git push origin main # This created a commit, so push it to main.

echo -n $text_1 > text_data.txt

git add .
git commit -m "add text data"
git push origin main
popd

# test truncate write into this pointer file and
# get the correct content.
git xet clone --lazy $remote repo_2

pushd repo_2
assert_is_pointer_file text_data.txt

xetfs_on 
[[ "$(x cat text_data.txt)" == "$text_1" ]] || die "t1.txt not read as pointer." 
