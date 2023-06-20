#!/bin/bash

# Test a repo that's the result of creating something with repo_upgrade_validation_create.sh script. 

set -e -x 

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
rm -f ./.gitconfig
. $SCRIPT_DIR/setup_run_environment.sh

git xet install
git config --global advice.detachedHead false

[[ -e ./cas/ ]] || die "CAS directory not present in current location."
[[ -e ./repo/ ]] || die "repo/ directory not present in current location."
[[ -e ./repo/origin ]] || die "repo/origin directory not present in current location."
[[ -e ./repo/test_repo ]] || die "repo/origin directory not present in current location."

test_not_dirty() {

  git add --renormalize ./
  
  status_out=$(git status --untracked-files=no -s)

  [[ -z $status_out ]] || die "Repository shows as dirty when it should be clean"  
}

test_all_consistent() {
  
  files=$(find . -name '*.hash')

  for f in $files ; do
     check_file_checksum ${f%.hash}
  done
}

test_summary_consistency() {
  # TODO: we should test the accuracy here?
  [[ ! -z $(git xet dir-summary --read-notes-only) ]] || die "Notes empty."
  [[ ! -z $(git xet dir-summary) ]] || die "Notes empty."
}

test_current_repo_state() {
  test_not_dirty || exit 1
  test_all_consistent || exit 1
  test_summary_consistency || exit 1
}

test_full_repo() {
  only_head=$1
  test_current_repo_state || exit 1
  
  if [[ $only_head != "1" ]] ; then

  commits=$(git log --format="%H" | sed '$ d' | sort)

  for c in $commits ; do
    git checkout $c
    test_current_repo_state || exit 1
  done 
  fi 
  # Return to correct state.
  git checkout main
}

add_data_to_repo() { 
  echo "Adding data: Write some CSVs out to test that path."
  set +x
  create_random_csv_file csv_data_1.csv 32 1000
  write_file_checksum csv_data_1.csv
  set -x
  git add csv_data_1.csv*

  echo "New data"
  set +x
  create_random_data_file binary_data_4.dat 10000
  write_file_checksum binary_data_4.dat
  set -x
  git add binary_data_4.dat*

  echo "Write some stuff that's duplicated between files"
  set +x
  cat binary_data_1.dat >> binary_data_3.dat
  cat binary_data_4.dat >> binary_data_3.dat
  write_file_checksum binary_data_3.dat
  set -x
  git add binary_data_3.dat*

  git commit -m "Update to repo." --quiet 
}


# Ok, first, make a couple snapshots of the current one to fully test a couple of different workflows.
rm -rf repo_1/
cp -r repo/ repo_1/

rm -rf repo_2/
cp -r repo/ repo_2/

# See if it's fine as is
pushd repo_1/test_repo/
test_full_repo 1
popd

pushd repo_1/
git clone ./origin test_repo_2

echo "Now if a cloned one is good. "
cd test_repo_2
test_full_repo 1
popd

echo "Next, see if adding data to the repo makes sense "
pushd repo_1/test_repo/
add_data_to_repo
test_full_repo 0
git push origin main
popd

echo "Now, test that the push and pull works fine. "
pushd repo_1/test_repo_2/
git pull origin main
test_full_repo 0
popd

echo  "Now, test modifications on top of a completely fresh repo"
pushd repo_2/test_repo/
add_data_to_repo
git push origin main
test_full_repo 0
popd

pushd repo_2
git clone origin test_repo_2
cd test_repo_2
test_full_repo 1
popd











