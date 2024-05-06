#!/usr/bin/env bash
set -e

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"
setup_basic_run_environment

git xet install 

remote=$(create_bare_xet_repo)

# clone the repo
git clone $remote repo_1


function test_summary_is_present () {
  dumped_summary=$(git xet summary dump)

  [[ "$dumped_summary" == *"csv"* ]] || die "csv tag not found in dumped summary."
  [[ "$dumped_summary" == *"ABXYZ"* ]] || die "ABXYZ tag not found in dumped summary."
  [[ "$dumped_summary" == *"CDXYZ"* ]] || die "CDXYZ tag not found in dumped summary."
  [[ "$dumped_summary" == *"EFXYZ"* ]] || die "EFXYZ tag not found in dumped summary."
  [[ "$dumped_summary" == *"DGXYZ"* ]] || die "DGXYZ tag not found in dumped summary."
}

pushd repo_1

function create_csv_v1 () {
  data_header="C1, C2, C3"
data_content="
ABXYZ, 1.0, AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
CDXYZ, 3.0, AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
EFXYZ, 2.0, AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
DGXYZ, 1.0, AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

  # Just need to get in enough to cross the threshold where we smudge it.
  # 10000 repetitions of the above lines should be enough. 

  set +x
  echo "$data_header" > data.csv
  echo "$(printf "%s" "$data_content"{1..10000})" >> data.csv
  set -x
}

if [[ -e "$SCRIPT_DIR/files/data_v1.csv.gz" ]] ; then 
  cp "$SCRIPT_DIR/files/data_v1.csv.gz"  "./data.csv.gz"
  gunzip data.csv.gz || create_csv_v1
else
  create_csv_v1
fi

git add data.csv

git commit -a -m "Added data.csv"
git push origin main

test_summary_is_present || die "Summary in repo_1 is not present."

popd

# Does it make it through cloning etc.? 
git clone $remote repo_2

pushd repo_2
test_summary_is_present || die "Summary in repo_2 is not present."
popd 

git clone repo_1 repo_3

pushd repo_3
test_summary_is_present || die "Summary in repo_2 is not present."
popd 

# Now, let's update the notes and see what happens. 
# Just need to get in enough to cross the threshold where we smudge it. 

function test_updated_summary_is_present () {
  dumped_summary=$(git xet summary dump)

  [[ "$dumped_summary" == *"csv"* ]] || die "csv tag not found in dumped summary."
  [[ "$dumped_summary" == *"ABXYZ"* ]] || die "ABXYZ tag not found in dumped summary."
  [[ "$dumped_summary" == *"CDXYZ"* ]] || die "CDXYZ tag not found in dumped summary."
  [[ "$dumped_summary" == *"EFXYZ"* ]] || die "EFXYZ tag not found in dumped summary."
  [[ "$dumped_summary" == *"DGXYZ"* ]] || die "DGXYZ tag not found in dumped summary."
  [[ "$dumped_summary" == *"ABZZZ"* ]] || die "ABZZZ tag not found in dumped summary."
  [[ "$dumped_summary" == *"CDZZZ"* ]] || die "CDZZZ tag not found in dumped summary."
  [[ "$dumped_summary" == *"EFZZZ"* ]] || die "EFZZZ tag not found in dumped summary."
  [[ "$dumped_summary" == *"DGZZZ"* ]] || die "DGZZZ tag not found in dumped summary."
}

pushd repo_2

function create_csv_v2 () {
  
  create_csv_v1

  # Add in additional copies of the above. 
data_content_2="
ABZZZ, 1.0, AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
CDZZZ, 3.0, AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
EFZZZ, 2.0, AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
DGZZZ, 1.0, AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
  set +x
  echo "$(printf "%s" "$data_content_2"{1..50})" >> data.csv
  set -x
}

if [[ -e "$SCRIPT_DIR/files/data_v2.csv.gz" ]] ; then 
  cp "$SCRIPT_DIR/files/data_v2.csv.gz"  "./data.csv.gz"
  rm data.csv
  gunzip data.csv.gz || create_csv_v2
else
  create_csv_v2
fi

git commit -a -m "Updated data."
test_updated_summary_is_present || "Updated summary not present."
git push origin main
popd

pushd repo_1
git fetch origin && git merge origin/main
test_updated_summary_is_present || "Updated summary not present for repo_1."
popd
