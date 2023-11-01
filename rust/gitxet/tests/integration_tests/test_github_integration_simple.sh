#!/usr/bin/env bash

# Set up the parameters
export XETTEST_CREATE_INITIAL_COMMIT=1
export XETTEST_CONFIG_ORIGIN_TYPE=github

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"
. "$SCRIPT_DIR/integration_test_setup.sh"


############################################################ 
# Test 1: simple repo with a clone, but no base files. 
new_bare_repo origin_a
git clone origin_a repo_a_1

# Now initialize the first repo as xet enabled.
init_repo_as_xet origin_a

# Make sure a pull enables the first repo as a xet repo; do this by adding a data file.
pushd repo_a_1
git pull origin
create_data_file d1.dat 1000
cp d1.dat ../
git add d1.dat
git commit -a -m "Added d1.dat"
git push origin main
popd

# Do another clone of origin_a without smudging to make sure that it's actually a pointer file
git xet clone origin_a repo_a_nosmudge --no-smudge
assert_is_pointer_file repo_a_nosmudge/d1.dat


############################################################ 
# Test 2: simple repo with a clone, then xet enabled after there are other files present.  
new_bare_repo origin_b
git clone origin_b repo_b_1

# Add a text file and a couple small data file
pushd repo_b_1
create_data_file d1.dat 10000
cp d1.dat ../
create_data_file d2.dat 10000
cp d2.dat ../
echo "I really love this stuff" >> t1.txt
cp t1.txt ../
git add d1.dat d2.dat t1.txt
git commit -a -m "Added d1.dat d2.dat t1.txt."
git push origin main
popd

# Now initialize the first repo as xet enabled.
init_repo_as_xet origin_b

# Make sure a pull enables the first repo as a xet repo; do this by adding a data file.
pushd repo_b_1
git fetch origin # Should enable as xet
git merge --no-edit origin/main
create_data_file d3.dat 10000
cp d3.dat ../
git add d3.dat
git commit -m "Added d3.dat"
git push origin main
assert_files_equal t1.txt ../t1.txt
assert_files_equal d1.dat ../d1.dat
assert_files_equal d2.dat ../d2.dat
assert_stored_as_full_file d1.dat 
assert_stored_as_full_file d2.dat 
assert_stored_as_pointer_file d3.dat 
popd

# Do another clone of origin_a without smudging to make sure everything is consistent there. 
git xet clone origin_b repo_b_nosmudge_1 --no-smudge
assert_files_equal t1.txt repo_b_nosmudge_1/t1.txt
assert_files_equal d1.dat repo_b_nosmudge_1/d1.dat
assert_files_equal d2.dat repo_b_nosmudge_1/d2.dat
assert_is_pointer_file repo_b_nosmudge_1/d3.dat


# Now, overwrite the data files 
pushd repo_b_1
create_data_file d1.dat 10000
cp d1.dat ../
git add d1.dat
git commit -m "Added d1.dat"
assert_stored_as_pointer_file d1.dat 


# This should actually convert this to a pointer file now.
git add d2.dat
git commit -m "Re-added d2.dat"
assert_stored_as_pointer_file d2.dat 

git push origin main
popd

# Do another clone of origin_b without smudging to make sure that it's actually a pointer file
git xet clone origin_b repo_b_nosmudge_2 --no-smudge

# These files were there before the xet conversion, and should be data files.
assert_files_equal t1.txt repo_b_nosmudge_2/t1.txt
assert_is_pointer_file repo_b_nosmudge_2/d1.dat
assert_is_pointer_file repo_b_nosmudge_2/d2.dat

# These files were added after and should be pointer files 
assert_is_pointer_file repo_b_nosmudge_2/d3.dat

# Do another clone of origin_b and make sure it all works great. 
git xet clone origin_b repo_b_2
assert_files_equal t1.txt repo_b_2/t1.txt
assert_files_equal d1.dat repo_b_2/d1.dat
assert_files_equal d2.dat repo_b_2/d2.dat
assert_files_equal d3.dat repo_b_2/d3.dat

