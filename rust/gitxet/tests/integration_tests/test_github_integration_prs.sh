#!/usr/bin/env bash
export XETTEST_CREATE_INITIAL_COMMIT=1
export XETTEST_CONFIG_ORIGIN_TYPE=github

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"
. "$SCRIPT_DIR/integration_test_setup.sh"

# Github integration requires ensuring that git-xet is robust to the different scenarios that 
# we do not have control over.  We need to ensure that git-xet robustly handles mdb notes, the repo salt, 
# dedup, fileid lookup, and other processes through all the scenarios that could come up with github 
# integration.  
#
# The key aspect of the github integration is that within the repo, .xet/config.toml gives all the
# needed information required to reconstruct all the other parts of the repo implicitly. 
# These tests and the tests in the other two github integration test scripts set up and 
# run through various combinations of cloning, forking, merging, pull requests, and local copying 
# to ensure that in all scenarios, the client can use .xet/config.toml to robustly handle things.



############################################################ 
# Setup: Forks before and after xet init, but no data.  
new_bare_repo origin_a

# Pre xet
simulate_fork origin_a origin_b

# Enable it as a xet repo 
init_repo_as_xet origin_a

# Save this for later, to make sure we can do all the pulls right with PRs
git clone origin_a repo_a_1

# Post xet
simulate_fork origin_a origin_c

############################################################ 
# Test 1:  A PR from origin_c to origin_a works fine. 

git clone origin_c repo_c_1

pushd repo_c_1
create_data_file d1.dat 10000
cp d1.dat ../
git add d1.dat
git commit -m "Added d1.dat."
git push origin main
assert_stored_as_pointer_file d1.dat
popd

simulate_pr_merge origin_c origin_a

############################################################ 
# Test 2:  A PR from origin_b, without XET enabled, to origin_a works fine.

git clone origin_b repo_b_1

pushd repo_b_1
echo "Test file" >> t1.txt
cp t1.txt ../
git add t1.txt
git commit -m "Added t1.txt."
git push origin main
popd

simulate_pr_merge origin_b origin_a

############################################################ 
# Test 3:  A PR from a downstream one works fine.

simulate_fork origin_c origin_ca

git clone origin_ca repo_ca_1

pushd repo_ca_1
create_data_file d2.dat 10000
cp d2.dat ../
git add d2.dat
git commit -m "Added d2.dat."
git push origin main
assert_stored_as_pointer_file d2.dat
popd

simulate_pr_merge origin_ca origin_a

############################################################ 
# Test 4: A pull of the xet enable branch, back to a branch on a non-xet enable fork, then a PR

git clone origin_b repo_b_2

pushd repo_b_1

git remote add upstream ../origin_a
git fetch upstream
git checkout upstream/main
git checkout -b upstream_branch

create_data_file d3.dat 10000
cp d3.dat ../
git add d3.dat
git commit -m "Added d3.dat."
assert_stored_as_pointer_file d3.dat

git push origin upstream_branch

popd

# Merge origin_b/upstream_branch to origin_a/main
simulate_pr_merge origin_b origin_a upstream_branch main

############################################################ 
# Test 5: A PR merged into one another repo  

git clone origin_ca repo_ca_2

pushd repo_ca_2
create_data_file d4.dat 10000
cp d4.dat ../
git add d4.dat
git commit -m "Added d4.dat."
git push origin main
assert_stored_as_pointer_file d4.dat
popd

simulate_pr_merge origin_ca origin_c

############################################################ 
# Test that we can push and pull things from an old but xet enabled repo 

pushd repo_a_1
git remote add origin_c ../origin_c
git fetch origin_c
git merge --no-edit origin_c/main

# Does duplicating the file cause have any issues (i.e. make sure MDB stuff is merged correctly). 
cp d4.dat d4m.dat
git add d4m.dat
git commit -m "Added d4m.dat"
assert_stored_as_pointer_file d4m.dat

# Merge in the origin updates, then push the new stuff
git fetch origin 
git merge --no-edit origin/main

git push origin main
popd


############################################################ 
# Test original repo: everything showed up okay 

git clone origin_a repo_a_2

pushd repo_a_2
assert_files_equal d1.dat ../d1.dat
assert_files_equal d2.dat ../d2.dat
assert_files_equal d3.dat ../d3.dat
assert_files_equal d4.dat ../d4.dat
assert_files_equal d4m.dat ../d4.dat
assert_files_equal t1.txt ../t1.txt 

assert_stored_as_pointer_file d1.dat
assert_stored_as_pointer_file d2.dat
assert_stored_as_pointer_file d3.dat
assert_stored_as_pointer_file d4.dat
assert_stored_as_pointer_file d4m.dat

# Now, duplicate all these files and re-add them to make sure the merkle db trees are handled correctly.
cp d1.dat d1m.dat
cp d2.dat d2m.dat
cp d3.dat d3m.dat
cp d4.dat d4m1.dat
cp d4m.dat d4m2.dat

git add *.dat
git commit -m "Added more .dat files."
git push 

popd

# Now check that all these things work
git clone origin_a repo_a_3

pushd repo_a_3
assert_files_equal d1m.dat ../d1.dat
assert_files_equal d2m.dat ../d2.dat
assert_files_equal d3m.dat ../d3.dat
assert_files_equal d4m.dat ../d4.dat
assert_files_equal d4m1.dat ../d4.dat
assert_files_equal d4m2.dat ../d4.dat
popd


