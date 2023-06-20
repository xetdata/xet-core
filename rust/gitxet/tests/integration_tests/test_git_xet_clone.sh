#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"

remote=$(create_bare_repo)

[[ -z $(git config --global --get-regex 'filter.xet.*') ]] || die "Should not have global git config."

# Do a basic clone of the remote, but install git locally. 
git clone $remote repo_1
pushd repo_1
[[ $(git branch) == *"main"* ]] || git checkout -b main
git xet init --force
git xet install --local
cat /dev/urandom | head -c 100000 > data.dat
git add data.dat
git commit -m "Some data"
git push origin main
popd

[[ -z $(git config --global --get-regex 'filter.xet.*') ]] || die "Should not have global git config (2)."

# In this case, xet should not be installed.
git clone $remote repo_2
pushd repo_2
assert_is_pointer_file data.dat
popd 

[[ -z $(git config --global --get-regex 'filter.xet.*') ]] || die "Should not have global git config (3)."

# In this case, xet should not be installed.
git xet clone $remote repo_3
pushd repo_3
assert_files_equal data.dat ../repo_1/data.dat
popd 

[[ ! -z $(git config --global --get-regex 'filter.xet.*') ]] || die "Global config not set up like it should be."

# Verify that git xet clone dumps everything to the output correctly.
out_xet=$(git xet clone $remote repo_4 2>&1)

# Make sure that we've got the git clone output somewhere in the git xet clone output.
[[ $out_xet == *"Cloning into"* ]] || die "git clone output not part of git xet clone output."

# Check that no-smudge works. 
git xet clone --no-smudge $remote repo_5
pushd repo_5
assert_is_pointer_file data.dat





