#!/bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"
setup_basic_run_environment

mkdir src_repo

mkdir main
mkdir br1
mkdir br2
mkdir br3

pushd src_repo
git init
echo "Testing" > test.txt
git add *
git commit -a -m "Initial commit."

create_data_file data.dat 100
git add *
git commit -a -m "main."
cp * ../main/

git checkout -b br1
create_data_file data2.dat 100
git add *
git commit -a -m "br1"
cp * ../br1/

# Replace the original data file.
git checkout -b br2
create_data_file data.dat 100
git add *
git commit -a -m "br2"
cp * ../br2/

# Replace the second data file.
git checkout -b br3
create_data_file data3.dat 100
git add *
git commit -a -m "br3"
cp * ../br3/

# Add a bunch of notes there too. 
add_note_to_branch_head() {
    local repo=$1
    local branch=$2
    local note_content=$3

    git -C "$repo" checkout "$branch"
}

# Step 1: Add test notes to SRC_REPO
echo "Adding test notes to SRC_REPO..."

branches="main br1 br2 br3"

for branch in $branches ; do 

    git checkout $branch
    head_commit=$(git rev-parse HEAD)

    git notes add -m "Test note: $branch" $head_commit
done
popd

xet_remote=$(create_bare_repo)
pushd $xet_remote
git xet init --force
popd

git xet repo migrate --src=./src_repo --dest=$xet_remote --working-dir=./migration_working_dir --no-cleanup

# Now, do a clone from the local folder.
git xet clone $xet_remote migrated_repo

pushd ./migrated_repo

# Verify all the data files are correct.
git checkout main
assert_is_pointer_file data.dat
for f in * ; do 
    assert_files_equal $f ../main/$f
done 

# Verify all the data files are correct.
git checkout br1
assert_is_pointer_file data.dat
assert_is_pointer_file data2.dat
for f in * ; do 
    assert_files_equal $f ../br1/$f
done 

git checkout br2
assert_is_pointer_file data.dat
assert_is_pointer_file data2.dat
for f in * ; do 
    assert_files_equal $f ../br2/$f
done 

git checkout br3
assert_is_pointer_file data.dat
assert_is_pointer_file data2.dat
assert_is_pointer_file data3.dat
for f in * ; do 
    assert_files_equal $f ../br3/$f
done 

for branch in $branches ; do 
    git checkout $branch
    head_commit=$(git rev-parse HEAD)

    note_content="$(git notes show $head_commit || echo "NOT FOUND")"
    if [[ $note_content != "Test note: $branch" ]]; then
        die "Note for branch $branch not found or incorrect: ($note_content)"
    fi
done

