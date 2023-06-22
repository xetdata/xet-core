#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"

remote=$(create_bare_repo)

git xet install # Global config here.

git clone $remote repo_1 

pushd repo_1
[[ $(git branch) == *"main"* ]] || git checkout -b main
git xet init --force
git push origin main  # This created a commit, so push it to main. 
popd

# Clone origin.  Everything should be globally initialized.
git clone $remote repo_2 

# Add a new commit on top of the xet init commit.
pushd repo_1
create_data_file data.dat 10000
git add data.dat 
git commit -a -m "Adding data."
git push origin --set-upstream main
popd

pushd repo_2
git config pull.rebase false
git pull origin main
popd 

assert_files_equal repo_1/data.dat repo_2/data.dat



