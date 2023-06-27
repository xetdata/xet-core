#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"

remote=$(create_bare_repo)

git clone $remote repo_1

pushd repo_1
[[ $(git branch) == *"main"* ]] || git checkout -b main 
git xet init --force # Only worry about local configurations
create_data_file data.dat 10000
git add data.dat 
git commit -a -m "Adding data."
git push --set-upstream origin main
popd


# Set up repo 2

git clone $remote repo_2
pushd repo_2

# TODO: maybe this can be automatic as part of the filter process.
assert_is_pointer_file data.dat
assert_pointer_file_size data.dat 10000

git xet init --force
git xet checkout

assert_files_equal data.dat ../repo_1/data.dat

# Check that git xet repo size works
rsize=`git xet repo-size`
attrsize=`wc -c < .gitattributes`
echo $rsize
echo $attrsize
truesize=$(expr 10000 + $attrsize)
echo $truesize
echo $rsize
[[ $rsize == $truesize ]] || die "Assert Failed: git xet repo-size not reporting correct size."

# Check git xet repo-size detailed
detailedsize=$(git xet repo-size -d)
echo $detailedsize
# run it again and it should produce an identical string
detailedsize2=`git xet repo-size -d`
[[ $detailedsize == $detailedsize2 ]] || die "Assert Failed: git xet repo-size not reporting correct size."


# parse out the cas stored size line (10000)
cassize=$(git xet repo-size -d | grep cas_stored_size | cut -d ':' -f 2 | tr -d ', ')
echo $cassize
[[ $cassize == 10000 ]] || die "Assert Failed: git xet repo-size -d not reporting correct cas size."
# parse the total_bytes_of_modified_files_stored_in_cas value  (10000)
cassize=$(git xet repo-size -d | grep total_bytes_of_modified_files_stored_in_cas | cut -d ':' -f 2 | tr -d ', ')
echo $cassize
[[ $cassize == 10000 ]] || die "Assert Failed: git xet repo-size -d not reporting correct modified cas size."
# check merkledb cas stat  (10000)
casstat=$(git xet merkledb cas-stat | grep total_cas_bytes | cut -d ':' -f 2 | tr -d ', ')
echo $casstat
[[ $casstat == 10000 ]] || die "Assert Failed: git xet casstat not reporting correct cas size."


popd 




