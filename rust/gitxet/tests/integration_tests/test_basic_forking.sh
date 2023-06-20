#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"

git xet install 

remote=$(create_bare_repo)

# clone the repo
git clone $remote repo_origin

pushd repo_origin
git xet init --force
git push --set-upstream origin main
popd

git clone $remote origin_alt

# the equivalent of clicking fork on the web
cp -r origin_alt fork

# clone the fork
git clone fork repo_fork

# add in some sweet data to the origin
pushd repo_origin
create_data_file data.dat 10000
git add data.dat
git commit -a -m "First Commit."
git push
popd

# ensure we can fetch from upstream
pushd repo_fork
git remote add upstream $remote

git fetch upstream
git merge upstream/main -m "Merge upstream 1."

assert_files_equal data.dat ../repo_origin/data.dat
popd

# add an update and new file to source repo
pushd repo_origin
echo "updated" >> data.dat
create_data_file data2.dat 10000
git add data2.dat
git commit -a -m "Second Commit."
git push
popd


pushd repo_fork
git checkout -b feature_branch
create_data_file data3.dat 10000
git add data3.dat
git commit -a -m "Branch commit."
git push origin feature_branch
# now, let test merging upstream using pull and check to 
# see the changes
git fetch upstream
git merge upstream/main -m "Merging upstream 2"

assert_files_equal data2.dat ../repo_origin/data2.dat
assert_files_equal data.dat ../repo_origin/data.dat
git push origin feature_branch
# let's checkout main and not see the updates
git checkout main

[[ -e data2.dat ]] && die "data2.dat not deleted on branch switch."
[[ -e data3.dat ]] && die "data3.dat not deleted on branch switch."
assert_files_not_equal data.dat ../repo_origin/data.dat

# after syncing remote, we should see updates
git fetch upstream
git merge upstream/main -m "Merging upstream."

assert_files_equal data2.dat ../repo_origin/data2.dat
assert_files_equal data.dat ../repo_origin/data.dat
popd
