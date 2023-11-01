# Allows pushing to this repo.
git config --global receive.denyCurrentBranch ignore
git config --global push.autoSetupRemote true

# Make sure the filter is configured globally.
git xet install 

base_dir="`pwd`"
mkdir config_files/

new_bare_repo() {

    local name="$1"
    local repo_dir="$base_dir/$name"

    rm -rf "$repo_dir"
    mkdir "$repo_dir"
    pushd "$repo_dir"

    git init --bare

    popd

    pushd "$base_dir"

    # Now, github creates an initial commit, so do that here too. 
    if [[ ! -z $XETTEST_CREATE_INITIAL_COMMIT ]] ; then
        
        local name_alt="${name}_alt"
        local repo_dir_alt="$base_dir/$name_alt"

        git clone $repo_dir $name_alt
        pushd $repo_dir_alt
        echo "" >> README.md
        git add README.md
        git commit -m "First commit"
        git push origin main
        popd
    fi
    
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

    config_file_name="$base_dir/config_files/xet_config_file_${name}.toml"
    
    echo "[upstream]
    origin_type = \"${XETTEST_CONFIG_ORIGIN_TYPE}\"
    url = \"${repo_dir}\"
    " > "${config_file_name}" 

    git xet init -m 2 --force --explicit --write-repo-salt --write-gitattributes --xet-config-file="${config_file_name}"

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

