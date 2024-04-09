#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"
setup_basic_run_environment

remote=$(create_bare_xet_repo)
git xet install


git clone $remote repo_1 
git clone $remote repo_2


pushd repo_1
create_data_file data.dat 1000
git add data.dat 
git commit -a -m "data on main"
git push 
popd

pushd repo_2
git fetch origin 
git pull origin main --ff-only
popd

pushd repo_1
git checkout -b br1
create_data_file data_1.dat 1000
git add data_1.dat 
git commit -a -m "data_1 on br1"
git push --set-upstream origin br1
popd

pushd repo_2
git fetch origin 
git merge origin/br1 --no-edit
popd 

assert_files_equal repo_1/data_1.dat repo_2/data_1.dat
mkdir -p temp
cp repo_1/data_1.dat temp/

pushd repo_1
git checkout main
git checkout -b br2
create_data_file data_2.dat 1000
git add data_2.dat 
git commit -a -m "data_2 on br2"
git push --set-upstream origin br2
popd

pushd repo_2
git fetch origin 
git merge origin/br2 --no-edit
popd 

assert_files_equal repo_1/data_2.dat repo_2/data_2.dat
assert_files_equal temp/data_1.dat repo_2/data_1.dat
assert_files_equal repo_1/data.dat repo_2/data.dat

pushd repo_2
git reset --hard origin/br1
popd 

[[ ! -e repo_2/data_2.dat ]] || die "repo_2/data_2.dat not deleted."

assert_files_equal temp/data_1.dat repo_2/data_1.dat
assert_files_equal repo_1/data.dat repo_2/data.dat

pushd repo_2
rm data.dat
git commit -a -m "Deleted data.dat"
git push origin main
popd 

pushd repo_1
git checkout main
git pull origin main --ff-only
[[ ! -e data.dat ]] || die "repo_1/data.dat not deleted."
popd 


