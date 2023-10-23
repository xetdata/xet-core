#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"

# Allows pushing to this repo.
git config --global receive.denyCurrentBranch ignore
git config --global push.autoSetupRemote true


# Make sure the filter is configured globally.
git xet install 


base_dir="`pwd`"


new_bare_repo() { 
    local name="$1"
    local repo_dir="$base_dir/$name"

    rm -rf "$repo_dir"
    mkdir "$repo_dir"
    pushd "$repo_dir"

    git init --bare

    popd
}

# Use the local versions of the 
new_repo() {
    local name="$1"
    local repo_dir="$base_dir/$name"

    rm -rf "$repo_dir"
    mkdir "$repo_dir"
    pushd "$repo_dir"

    git init


    popd
}

create_file_in_repo() {
    local name="$1"
    local repo_dir="$base_dir/$name"
    local file_name="$repo_dir/$2"

    pushd "$repo_dir"
    create_data_file "$file_name" 100000
    git add "$file_name"
    git commit -m "Added file $file_name"
    popd
}

init_repo_as_xet () { 
    local name="$1"
    local repo_dir="$base_dir/$name"
    pushd "$repo_dir"

    git xet init -m 2 --force --minimal --skip-filter-config

    popd
}

intermediate_counter=0

new_tmp_bare_repo () { 
    local tmp_repo="tmp_repo_${intermediate_counter}"
    ((intermediate_counter++))
    >&2 new_bare_repo ${tmp_repo}
    echo ${tmp_repo}
}

new_tmp_repo () { 
    local tmp_repo="tmp_repo_${intermediate_counter}"
    ((intermediate_counter++))
    >&2 new_repo ${tmp_repo}
    echo ${tmp_repo}
}


name_conuter=0
new_name() { 
    base=$1
    ((name_counter++))
    echo "base_${name_counter}"
}


do_push() { 
    local repo_1="$1"
    local repo_dir_1="$base_dir/$repo_1"

    local repo_2="$2"
    local repo_dir_2="$base_dir/$repo_2"

    pushd "$repo_dir_1"
    git push --force "$repo_dir_2" 
    popd
}

simulate_pr_merge() { 
    local repo_1="$1"
    local repo_dir_1="$base_dir/$repo_1"

    local repo_2="$2"
    local repo_dir_2="$base_dir/$repo_2"

    local branch=${3:-main}
    local remote_branch=${4:-main}

    # Create a temporary repo from which to do the merge. 
    tmp_repo=$(new_tmp_repo)
    pushd "$tmp_repo"

    # Disable git from running in the intermediate bit
    git config --local filter.xet.process ""
    git config --local filter.xet.required false

    git remote add 1 "$repo_dir_1"
    git remote add 2 "$repo_dir_2"

    git fetch 1
    git fetch 2

    git reset --hard 1/$branch
    git merge --no-edit 2/$remote_branch
    git push 2
    popd
}


# Create a new bare repo that simulates a fork of the original repo.
simulate_fork() {
    local repo_1="$1"
    local repo_dir_1="$base_dir/$repo_1"

    local repo_2="$2"
    local repo_dir_2="$base_dir/$repo_2"

    # Create a temporary new bare repo to push things to it.
    local tmp_repo=$(new_tmp_bare_repo)
    local tmp_repo_dir="$base_dir/$tmp_repo"

    do_push $repo_1 $tmp_repo 

    # Now go to the bare repo, which doesn't have the hooks or filter or anything, 
    # and simply push from that to the new repo.  This should drop all the refs and 
    # stuff.
    new_bare_repo $repo_2

    do_push $tmp_repo $repo_2 
}



# Scenario one.  Simple forking with a new repo.
new_bare_repo origin_a 
git clone origin_a repo_a_1

# Add a text file, as if it's an existing repo 
# Push the git change 
pushd repo_a_1
echo "I love this stuff" >> t1.txt
git add t1.txt
git commit -a -m "Added base text commit."
git push origin main
popd

# Create some clones for a number of testing scenarios
git clone origin_a repo_a_2
git clone origin_a repo_a_3
git clone origin_a repo_a_4
git clone origin_a repo_a_5

# Simulate a fork before the xet conversion happens 
simulate_fork origin_a origin_b1
simulate_fork origin_a origin_b2

# Turn origin_a into a xet repo 
init_repo_as_xet origin_a

# Create some clones and forks in this scenario
git clone origin_a repo_a_X1
git clone origin_a repo_a_X2

# Simulate a fork before the xet conversion happens 
simulate_fork origin_a origin_x1
simulate_fork origin_a origin_x2

# Push another text file
pushd repo_a_1
git pull origin
echo "I really love this stuff" >> t2.txt
git add t2.txt
git commit -a -m "Added another text commit."
git push origin main
popd

# STATE:
#  origin_a: XET, no data
#    repo_a_1: XET, no data
#    repo_a_2: NO XET, no data
#    repo_a_3: NO XET, no data
#    repo_a_4: NO XET, no data
#    repo_a_5: NO XET, no data
#    repo_a_X1: XET, no data
#    repo_a_X2: XET, no data
#  origin_b1: NO XET, no data
#  origin_b2: NO XET, no data
#  origin_x1: XET, no data

# In the first repo, create a file and push that change.
pushd repo_a_1
create_data_file d1.dat 1000
cp d1.dat ../
git add d1.dat
git commit -a -m "Added d1.dat"
git push origin main
popd

# Do another clone without smudging to make sure that it's actually a pointer file
git xet clone origin_a repo_a_nosmudge --no-smudge
assert_is_pointer_file repo_a_nosmudge/d1.dat

# Attempt to pull in things to repo_2; this should create the xet commit and 
# then the files should be the same.
pushd repo_a_2
git pull
popd

assert_files_equal d1.dat repo_a_2/d1.dat

# STATE:
#  origin_a: XET, D1
#    repo_a_1: XET, D1
#    repo_a_2: XET, D1
#    repo_a_3: NO XET, no data
#    repo_a_4: NO XET, no data
#    repo_a_5: NO XET, no data
#    repo_a_X1: XET, no data
#    repo_a_X2: XET, no data
#  origin_b1: NO XET, no data
#  origin_b2: NO XET, no data
#  origin_x1: XET, no data
#  origin_x2: XET, no data


# make sure that a merge works too.
pushd repo_a_3
echo "More text stuff" >> t3.txt
git add t3.txt
git commit -a -m "Added new text file."

git fetch origin 
git merge --no-edit origin/main
popd

assert_files_equal d1.dat repo_a_3/d1.dat

# STATE:
#  origin_a: XET, D1
#    repo_a_1: XET, D1
#    repo_a_2: XET, D1
#    repo_a_3: XET, D1
#    repo_a_4: NO XET, no data
#    repo_a_5: NO XET, no data
#    repo_a_X1: XET, no data
#    repo_a_X2: XET, no data
#  origin_b1: NO XET, no data
#  origin_b2: NO XET, no data
#  origin_x1: XET, no data
#  origin_x2: XET, no data


# Now, pretend that repo_a_4 actually tries to pick up the conversion on a branch from origin_c, then adds a data file, then merges in new data in origin_a, 
# then pushes its new data to origin_a
pushd repo_a_4
git remote add xet_try ../origin_x1
git fetch xet_try
git merge --no-edit xet_try/main

create_data_file d2.dat 1000
cp d2.dat ../
git add d2.dat
git commit -a -m "Added d2.dat"
# Push up to xet_try
git push xet_try

git fetch origin
git merge --no-edit origin/main

git push origin
popd

# STATE:
#  origin_a: XET, D1 + D2
#    repo_a_1: XET, D1
#    repo_a_2: XET, D1
#    repo_a_3: XET, D1
#    repo_a_4: XET, D1 + D2
#    repo_a_5: NO XET, no data
#    repo_a_X1: XET, no data
#    repo_a_X2: XET, no data
#  origin_b1: NO XET, no data
#  origin_b2: NO XET, no data
#  origin_x1: XET, D2 
#  origin_x2: XET, no data 

# Now, make sure that repo_a_1 can get the new data from origin_a. 
pushd repo_a_1
git fetch origin
git merge --no-edit origin/main 
popd

assert_files_equal repo_a_1/d2.dat d2.dat

# Now, make sure that repo_a_2 can get the new data from origin_x1. 
pushd repo_a_2
git remote add xet_try ../origin_x1
git fetch xet_try
git merge --no-edit xet_try/main
popd

assert_files_equal repo_a_2/d2.dat d2.dat
assert_files_equal repo_a_1/d1.dat d1.dat

# STATE:
#  origin_a: XET, D1 + D2
#    repo_a_1: XET, D1 + D2
#    repo_a_2: XET, D1 + D2
#    repo_a_3: XET, D1
#    repo_a_4: XET, D1 + D2
#    repo_a_5: NO XET, no data
#    repo_a_X1: XET, no data
#    repo_a_X2: XET, no data
#  origin_b1: NO XET, no data
#  origin_b2: NO XET, no data
#  origin_x1: XET, D2 
#  origin_x2: XET, no data 


# Create a new repo based off of x1 
git clone origin_x1 repo_x1_1  # Will have D2 only
git clone origin_x1 repo_x1_2  # Will have D2 only
assert_files_equal repo_x1_1/d2.dat d2.dat 

# Do a a pull from upstream 
pushd repo_x1_1
git remote add upstream ../origin_a
git fetch upstream
git merge --no-edit upstream/main
git push origin

# Add new data here.
create_data_file d3.dat 1000
cp d3.dat ../d3.dat
git add d3.dat
git commit -a -m "Added d3.dat"
git push origin
popd

# STATE:
#  origin_a: XET, D1 + D2
#    repo_a_1: XET, D1 + D2
#    repo_a_2: XET, D1 + D2
#    repo_a_3: XET, D1
#    repo_a_4: XET, D1 + D2
#    repo_a_5: NO XET, no data
#    repo_a_X1: XET, no data
#    repo_a_X2: XET, no data
#  origin_b1: NO XET, no data
#  origin_b2: NO XET, no data
#  origin_x1: XET, D2 + D1 + D3 
#    repo_x1_1: XET, D1 + D2 + D3
#    repo_x1_2: XET, D2
#  origin_x2: XET, no data 

# Now, simulate a pr merge!   
simulate_pr_merge origin_x1 origin_a

# See if that works with a clone
git clone origin_a repo_a_6
assert_files_equal repo_a_6/d1.dat d1.dat 
assert_files_equal repo_a_6/d2.dat d2.dat 
assert_files_equal repo_a_6/d3.dat d3.dat 

# See if that works with a merge 
pushd repo_a_1
git fetch origin
git merge --no-edit origin/main 
popd
assert_files_equal repo_a_1/d1.dat d1.dat 
assert_files_equal repo_a_1/d2.dat d2.dat 
assert_files_equal repo_a_1/d3.dat d3.dat 

# See if it works with a merge from something older. 
pushd repo_a_2
git fetch origin
git merge --no-edit origin/main 
popd
assert_files_equal repo_a_2/d1.dat d1.dat 
assert_files_equal repo_a_2/d2.dat d2.dat 
assert_files_equal repo_a_2/d3.dat d3.dat 

# See if it works with a merge from an old non-xet enabled repo.
pushd repo_a_5
git fetch origin
git merge --no-edit origin/main 
popd
assert_files_equal repo_a_5/d1.dat d1.dat
assert_files_equal repo_a_5/d2.dat d2.dat
assert_files_equal repo_a_5/d3.dat d3.dat


# Now, see if repo_x1_2 (XET, D2) can get the new things from origin_a since it's a clone of origin_x1
pushd repo_x1_2
git remote add upstream ../origin_a
git fetch upstream
git merge --no-edit upstream/main
popd
assert_files_equal repo_x1_2/d1.dat d1.dat
assert_files_equal repo_x1_2/d2.dat d2.dat
assert_files_equal repo_x1_2/d3.dat d3.dat

# Now see if clones of the other forks can pull from any of the xet enabled remotes and work fine
git clone origin_b1 repo_b1_1
pushd repo_b1_1
git remote add upstream ../origin_a
git fetch upstream
git merge --no-edit upstream/main
git push origin
popd
assert_files_equal repo_b1_1/d1.dat d1.dat
assert_files_equal repo_b1_1/d2.dat d2.dat
assert_files_equal repo_b1_1/d3.dat d3.dat

# Now, make sure that origin can be cloned and have it work fine. 
git clone origin_b1 repo_b1_2
assert_files_equal repo_b1_2/d1.dat d1.dat
assert_files_equal repo_b1_2/d2.dat d2.dat
assert_files_equal repo_b1_2/d3.dat d3.dat

# Work with a sideways merge then a PR push across that?
git clone origin_b2 repo_b2_1
pushd repo_b2_1
git remote add upstream ../repo_a_4
git fetch upstream 
git merge --no-edit upstream/main
create_data_file d4.dat 1000
cp d4.dat ../
git add d4.dat
git commit -a -m "Added d4.dat"
[[ ! -z $(git show HEAD:d4.dat | grep "xet version") ]] || die "d4.dat is not a pointer file."

# Push to origin_b2
git push origin
popd

# Now do a merge from a not up to date thing.
simulate_pr_merge origin_b2 origin_x1

git clone origin_x1 repo_x1_3
assert_files_equal repo_x1_3/d1.dat d1.dat
assert_files_equal repo_x1_3/d2.dat d2.dat
assert_files_equal repo_x1_3/d3.dat d3.dat
assert_files_equal repo_x1_3/d4.dat d4.dat

simulate_pr_merge origin_b2 origin_a

git clone origin_a repo_a_7
assert_files_equal repo_a_7/d1.dat d1.dat
assert_files_equal repo_a_7/d2.dat d2.dat
assert_files_equal repo_a_7/d3.dat d3.dat
assert_files_equal repo_a_7/d4.dat d4.dat

# Go to repo_a_4 and see about a merge back.
pushd repo_a_4
git remote add upstream_x1 ../repo_b2_1
git fetch upstream_x1
git merge upstream_x1/main
popd

assert_files_equal repo_a_4/d4.dat d4.dat

pushd repo_a_4
git remote add upstream_x2 ../origin_x1
git fetch upstream_x2
git merge upstream_x2/main
popd

assert_files_equal repo_a_4/d1.dat d1.dat
assert_files_equal repo_a_4/d2.dat d2.dat
assert_files_equal repo_a_4/d3.dat d3.dat
assert_files_equal repo_a_4/d4.dat d4.dat









