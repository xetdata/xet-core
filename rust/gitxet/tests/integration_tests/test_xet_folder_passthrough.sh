#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"
setup_basic_run_environment

mkdir repo
pushd repo
git init
git xet init --force

# Make sure that all the subcontents of .xet are correctly excluded, 
# while other locations are not.  
mkdir -p .xet/subdir/
mkdir -p not_xet/subdir/

create_data_file ../data.dat 1000000

cp ../data.dat .xet/data.dat
cp ../data.dat .xet/subdir/data.dat
cp ../data.dat not_xet/data.dat
cp ../data.dat not_xet/subdir/data.dat
git add .xet/ not_xet/
git commit -a -m "Initial Commit."

# Now, get the stored part for these. 
git show HEAD:.xet/data.dat > ../data_r1.dat
git show HEAD:.xet/subdir/data.dat > ../data_r2.dat
git show HEAD:not_xet/data.dat > ../data_r3.dat
git show HEAD:not_xet/subdir/data.dat > ../data_r4.dat
popd

assert_files_equal data.dat data_r1.dat
assert_files_equal data.dat data_r2.dat
assert_is_pointer_file data_r3.dat
assert_is_pointer_file data_r4.dat
