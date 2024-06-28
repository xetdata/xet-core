#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]:-$0}")" &>/dev/null && pwd 2>/dev/null)"
. "$SCRIPT_DIR/initialize.sh"

setup_testing_environment
setup_xetldfs "$LDPRELOAD_LIB"

remote=$(create_bare_repo)

git clone $remote repo_1

# file larger than 100 bytes will be checked-in as pointer file
export XET_CAS_SIZETHRESHOLD=100

pushd repo_1
[[ $(git branch) == *"main"* ]] || git checkout -b main
git xet init --force
git push origin main # This created a commit, so push it to main.

create_text_file text_data.txt key1 1000

echo -n "some10char" >> text_data.txt
text_data_file_size=$(file_size text_data.txt)
git add .
git commit -m "add text data"
git push origin main
popd

# test "cat" this pointer file and get the materialized content.
git xet clone --lazy $remote repo_2

pushd repo_2
assert_is_pointer_file text_data.txt

# Test the interposed thing works
interposed_file_size=$(with_xetfs file_size text_data.txt)
[[ $interposed_file_size -eq $text_data_file_size ]] || die "interposed fstat/stat failed"
[[ $(with_xetfs $x_cat text_data.txt | tail -c 10) == "some10char" ]] || die "read pointer file failed"

# Test the interposed thing works
assert_is_pointer_file text_data.txt
[[ $(with_xetfs x cat_mmap text_data.txt | tail -c 10) == "some10char" ]] || die "read pointer file with mmap interposed failed"

assert_is_not_pointer_file text_data.txt

# test materialize this pointer file and "cat" and get the correct content.
[[ $(with_xetfs $x_cat text_data.txt | tail -c 10) == "some10char" ]] || die "read materialized file failed"
popd
