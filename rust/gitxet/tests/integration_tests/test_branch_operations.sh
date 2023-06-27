#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"

remote=$(create_bare_repo)

git clone $remote repo_1

mkdir temp # place to store the temporary files for later equality comparison.

pushd repo_1
git xet init --force 
create_data_file data.dat 10000
cp data.dat ../temp/
git add data.dat
git commit -a -m "First Commit."

# Check switching branches works
git checkout -b branch_1
create_data_file data_1.dat 10000
git add data_1.dat
cp data_1.dat ../temp/
git commit -a -m "Branch_1 Commit."

git checkout main
[[ ! -e data_1.dat ]] || die "data_1.dat not deleted."

git checkout -b branch_2
create_data_file data_2.dat 10000
git add data_2.dat
cp data_2.dat ../temp/
git commit -a -m "Branch_2 Commit."

git checkout branch_1
[[ ! -e data_2.dat ]] || die "data_2.dat not deleted after branch switch."
assert_files_equal data.dat ../temp/data.dat
assert_files_equal data_1.dat ../temp/data_1.dat

git checkout branch_2
[[ ! -e data_1.dat ]] || die "data_1.dat not deleted after branch switch."

assert_files_equal data.dat ../temp/data.dat
assert_files_equal data_2.dat ../temp/data_2.dat

git checkout branch_1 -- data_1.dat
assert_files_equal data.dat ../temp/data.dat
assert_files_equal data_1.dat ../temp/data_1.dat
assert_files_equal data_2.dat ../temp/data_2.dat

# Test reset
git reset --hard HEAD
git checkout branch_1
git reset --hard
assert_files_equal data.dat ../temp/data.dat
assert_files_equal data_1.dat ../temp/data_1.dat
[[ ! -e data_2.dat ]] || die "data_1.dat not deleted after reset."

# Test non-conflicting merge. 
git merge branch_2 --no-edit
assert_files_equal data.dat ../temp/data.dat
assert_files_equal data_1.dat ../temp/data_1.dat
assert_files_equal data_2.dat ../temp/data_2.dat

popd







