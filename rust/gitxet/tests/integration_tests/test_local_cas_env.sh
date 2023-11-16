#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"
setup_basic_run_environment

cas_dir=${PWD}/cas

[[ $XET_CAS_SERVER == "local://$cas_dir"* ]] || die "XET_CAS_SERVER = '$XET_CAS_SERVER'"

remote=$(create_bare_repo)
git xet install

git xet clone $remote repo_1

cd repo_1 
git xet init --force
create_data_file data.dat 1000000
git add data.dat
git commit -a -m "Added data."
git push origin main

# Now, make sure that the cas folder has something in it. 
[[ ! -z $(ls -A "$cas_dir") ]] || die "Local CAS dir is empty after running command above."


