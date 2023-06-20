#!/bin/bash

set -e

# This script creates a new repository with a bunch of different features.  The goal 
# is to have complete test coverage over the inner workings of git-xet that may 
# be affected by updates.

# On each new version of git xet:
# 1. Run this script in a clean directory in order to create a new repository and tarball 
#    packaged with the current version of git-xet.  
# 2. Copy this tarball into <XETHUB>/rust/gitxet/tests/upgrade_consistency_tests/repos/
# 3. Add a test_vXXXX test in <XETHUB>/rust/gitxet/tests/integration_tests.rs::git_upgrade_consistency_tests.  
#    This test should just follow the form of the tests already present.



SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";

if [[ $# != 1 ]] ; then 
  echo "Usage: $0 <package_name>"
  exit 1
fi


base_dir=$1

if [[ -e $base_dir ]] ; then 
  >&2 echo "Directory $base_dir/ exists."
  exit 1
fi

echo "" > $base_dir.tar.bz2 || die "Unable to write to file $base_dir.tar.bz2." 


mkdir -p $base_dir
pushd $base_dir

if [[ ! -z "$(ls -A ./)" ]] ; then 
  echo "Directory $base_dir not empty." 
  exit 1
fi

mkdir cas/ 
mkdir repo/

. $SCRIPT_DIR/setup_run_environment.sh

# List out the scripts that 
echo "repo_upgrade_validation_test.sh" > scripts.txt

cd repo/
remote=$(create_bare_repo)
git xet install

echo "Using CAS setup: XET_CAS_SERVER=$XET_CAS_SERVER"

git clone $remote test_repo/
cd test_repo/
git xet init --force


# This is important, as by default the relative URLs are not stored. 
git remote set-url origin ../origin/

# Add in the version information so the test script can run that. 
echo 0 > version.txt
git add version.txt

echo "repo_upgrade_validation_test.sh" > scripts.txt
git add scripts.txt

echo "Creating files."

# Create a data set and and associated checksum 
# checked into the repo.
create_data_file binary_data_1.dat key1 100000
write_file_checksum binary_data_1.dat
check_file_checksum binary_data_1.dat
git add binary_data_1.dat*

# Write a small binary file
create_data_file  binary_data_2.dat key4 1000
write_file_checksum binary_data_2.dat
check_file_checksum binary_data_2.dat
git add binary_data_2.dat*

# Write some CSVs out to test that path.
create_csv_file csv_data_1.csv key2 200 500
write_file_checksum csv_data_1.csv
check_file_checksum csv_data_1.csv
git add csv_data_1.csv*

create_csv_file csv_data_2.csv key3 100 1
write_file_checksum csv_data_2.csv
check_file_checksum csv_data_2.csv
git add csv_data_2.csv*

# Write some stuff that's duplicated between files
cat binary_data_1.dat binary_data_2.dat > binary_data_3.dat
write_file_checksum binary_data_3.dat
check_file_checksum binary_data_3.dat
git add binary_data_3.dat*

# write out random text files.
create_text_file text_data_1.txt key4 100 1 
write_file_checksum text_data_1.txt
check_file_checksum text_data_1.txt
git add text_data_1.txt*

create_text_file text_data_2.txt key4 512 10000
write_file_checksum text_data_2.txt
check_file_checksum text_data_2.txt
git add text_data_2.txt*

# Now, commit all of these.
git commit -m "Initial Commit."
git push origin main

# Next, let's go through and change some things, append some things 
create_csv_file csv_data_3.csv key5 30 500
write_file_checksum csv_data_3.csv
check_file_checksum csv_data_3.csv
git add csv_data_3.csv*

# replace a file
rm binary_data_1.dat
create_data_file  binary_data_1.dat key6 50000
write_file_checksum binary_data_1.dat
check_file_checksum binary_data_1.dat
git add binary_data_1.dat*

git commit -m "Commit 2."
git push origin main

# Append to a file
cat binary_data_1.dat >> binary_data_3.dat
write_file_checksum binary_data_3.dat
check_file_checksum binary_data_3.dat
git add binary_data_3.dat*

# Rename the files
git mv binary_data_3.dat alt_binary_data_3.dat
git mv binary_data_3.dat.hash alt_binary_data_3.dat.hash

git commit -m "Commit 3."
git xet dir-summary --recursive
git push origin main

# Now, cache all the dir-summary stuff to fill that cache.

# Now, go up a level and package all this up
popd

# Make sure that the local CAS directory has all the stuff. 
[[ ! -z "$(ls -A $base_dir/cas/)" ]] || die "Local cas directory $base_dir/cas/ is empty."

# Scrub out the configuration stuff.
rm -rf $base_dir/.xet/ 
rm -rf $base_dir/.gitconfig
rm $base_dir.tar.bz2
tar cjf $base_dir.tar.bz2 $base_dir/ 

