#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"
setup_basic_run_environment

git xet install 

remote=$(create_bare_repo)

git clone $remote repo_1

pushd repo_1
[[ $(git branch) == *"main"* ]] || git checkout -b main 
git xet init --force

create_data_file data.dat 10000
git add data.dat 
git commit -a -m "Adding data."
git push --set-upstream origin main
popd


# Set up repo 2
export XET_NO_SMUDGE=1
git clone $remote repo_2
pushd repo_2

# These should be pointer files with the NO SMUDGE thing above
assert_is_pointer_file data.dat
assert_pointer_file_size data.dat 10000

# Check that git xet repo size works on no smudge checkouts
rsize=`git xet repo-size`
attrsize=`wc -c < .gitattributes`
echo $rsize
echo $attrsize
truesize=$(expr 10000 + $attrsize)
echo $truesize
echo $rsize
[[ $rsize == $truesize ]] || die "Assert Failed: git xet repo-size not reporting correct size."




