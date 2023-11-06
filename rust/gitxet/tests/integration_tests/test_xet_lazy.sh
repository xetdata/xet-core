#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]:-$0}")" &>/dev/null && pwd 2>/dev/null)"
. "$SCRIPT_DIR/initialize.sh"
git xet install

remote=$(create_bare_repo)

# create a repository with the below structure
# ROOT  - d.dat
#       - sub1
#           - d1.dat
#           - sub2
#               - d2.dat
git clone $remote repo_1

pushd repo_1

[[ $(git branch) == *"main"* ]] || git checkout -b main
git xet init --force

create_data_file d.dat 10000
mkdir sub1
create_data_file sub1/d1.dat 10000
mkdir sub1/sub2
create_data_file sub1/sub2/d2.dat 10000

git add .
git commit -a -m "Adding data."
git push

popd

git xet clone --lazy $remote repo_2

pushd repo_2

assert_is_pointer_file d.dat
assert_is_pointer_file sub1/d1.dat
assert_is_pointer_file sub1/sub2/d2.dat

git xet materialize d.dat
assert_files_equal d.dat ../repo_1/d.dat
assert_is_pointer_file sub1/d1.dat
assert_is_pointer_file sub1/sub2/d2.dat

git xet materialize sub1
assert_files_equal d.dat ../repo_1/d.dat
assert_files_equal sub1/d1.dat ../repo_1/sub1/d1.dat
assert_is_pointer_file sub1/sub2/d2.dat

git xet dematerialize sub1 -r
assert_files_equal d.dat ../repo_1/d.dat
assert_is_pointer_file sub1/d1.dat
assert_is_pointer_file sub1/sub2/d2.dat

git xet materialize . -r
assert_files_equal d.dat ../repo_1/d.dat
assert_files_equal sub1/d1.dat ../repo_1/sub1/d1.dat
assert_files_equal sub1/sub2/d2.dat ../repo_1/sub1/sub2/d2.dat

echo "sub1/d1.dat" >.git/xet/lazyconfig
git xet lazy apply
assert_is_pointer_file d.dat
assert_files_equal sub1/d1.dat ../repo_1/sub1/d1.dat
assert_is_pointer_file sub1/sub2/d2.dat

popd
