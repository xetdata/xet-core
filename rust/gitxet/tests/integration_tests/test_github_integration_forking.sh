#!/usr/bin/env bash
export XETTEST_CREATE_INITIAL_COMMIT=1
export XETTEST_CONFIG_ORIGIN_TYPE=github

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"
. "$SCRIPT_DIR/integration_test_setup.sh"


############################################################ 
# Setup: Forks before and after xet init, but no data.  
new_bare_repo origin_a

# Pre xet
simulate_fork origin_a origin_b

# Enable it as a xet repo 
init_repo_as_xet origin_a

# Post xet
simulate_fork origin_a origin_c

############################################################ 
# Test 1.  A non-xet clone of origin_a can become xet enabled from a pull from origin_a

git clone origin_b repo_b_1

pushd repo_b_1
git remote add origin_a ../origin_a
git pull origin_a main
create_data_file d1.dat 100
cp d1.dat ../
git add d1.dat
git commit -m "Added d1.dat."
git push origin main
assert_stored_as_pointer_file d1.dat
popd


############################################################ 
# Test 2.  A xet-enabled clone of post-xet-forked origin_c is xet enabled 
git clone origin_c repo_c_1
pushd repo_c_1
git remote add origin_a ../origin_a
git pull origin_a main
create_data_file d2.dat 100
cp d2.dat ../
git add d2.dat
git commit -m "Added d2.dat."
git push origin main
assert_stored_as_pointer_file d2.dat 
popd

############################################################ 
# Test 3.  Pulling from origin_b works 
pushd repo_c_1
git remote add origin_b ../origin_b
git fetch origin_b
git merge --no-edit origin_b/main
# Merge it back.
git push origin
assert_files_equal d1.dat ../d1.dat
assert_files_equal d2.dat ../d2.dat
popd


############################################################ 
# Test 4.  A clone of origin_c with all the new merges etc. works.
git clone origin_c repo_c_2

pushd repo_c_2
assert_files_equal d1.dat ../d1.dat
assert_files_equal d2.dat ../d2.dat
assert_stored_as_pointer_file d1.dat 
assert_stored_as_pointer_file d2.dat 
popd


############################################################ 
# Test 4.  A local clone of a local repo works, and interacts correctly with the remotes. 
git clone repo_b_1 repo_b_2
assert_files_equal d1.dat repo_b_2/d1.dat

pushd repo_b_2
git remote add origin_c ../origin_c
git fetch origin_c
git merge --no-edit origin_c/main
assert_stored_as_pointer_file d1.dat 
assert_stored_as_pointer_file d2.dat 
assert_files_equal d1.dat ../d1.dat
assert_files_equal d2.dat ../d2.dat
popd


# Now, on that new repo (since it's not a clone of one of the origins), create a branch, 
# commit to it, and push that to all of the origins to test that process out.
pushd repo_b_2
git checkout -b d3_branch
create_data_file d3.dat 100
cp d3.dat ../
git add d3.dat
git commit -m "Added d3.dat."
assert_stored_as_pointer_file d3.dat 

# Push to each of the origins
git push origin_c d3_branch

git remote add origin_b ../origin_b
git push origin_b d3_branch

git remote add origin_a ../origin_a
git push origin_a d3_branch

popd

# Now, from the others, do a fetch from one of these to get that branch. 
pushd repo_b_1
git fetch origin
git merge --no-edit origin/main
git merge --no-edit origin/d3_branch
assert_stored_as_pointer_file d3.dat 
assert_files_equal d1.dat ../d1.dat
assert_files_equal d2.dat ../d2.dat
assert_files_equal d3.dat ../d3.dat
popd


# Test 5: A clone from the origin works fine. 
git clone origin_a repo_a_1

pushd repo_a_1

git remote add origin_b ../origin_b
git fetch origin_b
git merge --no-edit origin_b/main

git remote add origin_c ../origin_c
git fetch origin_c
git merge --no-edit origin_c/main

assert_stored_as_pointer_file d1.dat 
assert_stored_as_pointer_file d2.dat 
assert_files_equal d1.dat ../d1.dat
assert_files_equal d2.dat ../d2.dat

# Now, make sure that the branchs all work okay
git checkout origin/d3_branch
assert_files_equal d3.dat ../d3.dat
assert_stored_as_pointer_file d3.dat 

git checkout -b d3_branch
git merge --no-edit main 
assert_files_equal d1.dat ../d1.dat
assert_files_equal d2.dat ../d2.dat
assert_files_equal d3.dat ../d3.dat
assert_stored_as_pointer_file d1.dat 
assert_stored_as_pointer_file d2.dat 
assert_stored_as_pointer_file d3.dat 

git checkout origin_b/d3_branch
assert_files_equal d3.dat ../d3.dat
assert_stored_as_pointer_file d3.dat 

git checkout -b d3_branch_a
git merge --no-edit main 
git merge --no-edit d3_branch  
assert_files_equal d1.dat ../d1.dat
assert_files_equal d2.dat ../d2.dat
assert_files_equal d3.dat ../d3.dat
assert_stored_as_pointer_file d1.dat 
assert_stored_as_pointer_file d2.dat 
assert_stored_as_pointer_file d3.dat 

# Now just test if we can merge everything together.  Just making sure it all still works.  Maybe overkill
git checkout origin_c/d3_branch
assert_files_equal d3.dat ../d3.dat
assert_stored_as_pointer_file d3.dat 

git checkout -b d3_branch_c
git merge --no-edit origin/d3_branch 
git merge --no-edit origin_b/d3_branch 
git merge --no-edit origin_c/main 
git merge --no-edit main 
assert_files_equal d1.dat ../d1.dat
assert_files_equal d2.dat ../d2.dat
assert_files_equal d3.dat ../d3.dat
assert_stored_as_pointer_file d1.dat 
assert_stored_as_pointer_file d2.dat 
assert_stored_as_pointer_file d3.dat 

popd
