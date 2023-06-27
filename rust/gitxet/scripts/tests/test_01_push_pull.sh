#!/usr/bin/env bash
set -e
set -x

script_dir=$(dirname "$0")/

$script_dir/create_test_repos.sh test_01_push_pull_dir/

cd test_01_push_pull_dir

pushd repo_1

cat /dev/random | head | md5 > test_file_small.dat
cat /dev/random | head -c 10000000 > test_file_big.dat
echo "#include <stdio>
" > test_file_code.cpp


git add test_file_*.dat

cat /dev/random | head | md5 > test_file_small.dat
# cat /dev/random | head -c 10000000 > test_file_big.dat

git commit -a -m "Initial commit."

git push origin main

popd


pushd repo_2

git config pull.rebase false

# TODO: remove 
# GIT_TRACE=1 GIT_TRACE2=1 
git pull origin main

cat /dev/random | head | md5 > test_file_small.dat
cat /dev/random | head | md5 > test_file_small_2.dat

git add test_file_*.dat
git commit -a -m "Update commit."

git push

git checkout -b new_branch

cat /dev/random | head | md5 > test_file_small.dat

git commit -a -m "New file."

git push origin new_branch

popd


pushd repo_1

git fetch origin

git reset --hard origin/new_branch





