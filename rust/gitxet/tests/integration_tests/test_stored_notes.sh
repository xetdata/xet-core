#!/usr/bin/env bash
set -e

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"

git xet install 

remote=$(create_bare_repo)

# clone the repo
git clone $remote repo_1

pushd repo_1
git xet init --force
git push --set-upstream origin main
popd

function test_summary_is_present () {
  dumped_summary=$(git xet summary dump)

  [[ "$dumped_summary" == *"csv"* ]] || die "csv tag not found in dumped summary."
  [[ "$dumped_summary" == *"ABXYZ"* ]] || die "ABXYZ tag not found in dumped summary."
  [[ "$dumped_summary" == *"CDXYZ"* ]] || die "CDXYZ tag not found in dumped summary."
  [[ "$dumped_summary" == *"EFXYZ"* ]] || die "EFXYZ tag not found in dumped summary."
  [[ "$dumped_summary" == *"DGXYZ"* ]] || die "DGXYZ tag not found in dumped summary."
}

pushd repo_1

echo "C1, C2, C3" >> data.csv

# Just need to get in enough to cross the threshold where we smudge it. 
for i in $(seq 10000) ; do 
  echo "ABXYZ, 1.0, AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" >> data.csv
  echo "CDXYZ, 3.0, AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" >> data.csv
  echo "EFXYZ, 2.0, AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" >> data.csv
  echo "DGXYZ, 1.0, AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" >> data.csv
done


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
for i in $(seq 50) ; do 
  echo "ABZZZ, 1.0, AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" >> data.csv
  echo "CDZZZ, 3.0, AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" >> data.csv
  echo "EFZZZ, 2.0, AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" >> data.csv
  echo "DGZZZ, 1.0, AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" >> data.csv
done

git commit -a -m "Updated data."
test_updated_summary_is_present || "Updated summary not present."
git push origin main
popd

pushd repo_1
git fetch origin && git merge origin/main
test_updated_summary_is_present || "Updated summary not present for repo_1."
popd
