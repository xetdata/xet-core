#!/usr/bin/env bash

export XET_LOG_LEVEL=debug
export XET_LOG_FORMAT=compact
export XET_DISABLE_VERSION_CHECK="1"

# Set up logging
export XET_PRINT_LOG_FILE_PATH=1
export XET_LOG_PATH="$PWD/logs/log_{timestamp}_{pid}.txt"

# With these, Log the filename, function name, and line number when showing where we're executing. 
set -o xtrace
export PS4='+($(basename ${BASH_SOURCE}):${LINENO}): ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'

setup_isolated_environment() { 

  # Set up local, self-contained config stuff to make sure the environment for the tests is hermetic.
  export GIT_CONFIG_GLOBAL="$PWD/.gitconfig"

  # This is needed as older versions of git only go to $HOME/.gitconfig and do not respect
  # the GIT_CONFIG_GLOBAL environment variable.  
  export HOME=$PWD
  export base_dir="$HOME"

  if [[ ! -e $GIT_CONFIG_GLOBAL ]] ; then
    echo "[user]
        name = Xet Tester
        email = test@xetdata.com
    " > $GIT_CONFIG_GLOBAL
  else 
    die "These tests may overwrite global settings; please run \
  them in a directory without a .gitconfig present."
  fi
  
  # Do some checks to make sure that everything is set up
  username=$(git config --get user.name || echo "")
  [[ ! -z $username ]] || die "Git config user.name not set."

  useremail=$(git config --get user.email || echo "")
  [[ ! -z $useremail  ]] || die "Git config user.email not set."

  git config --global init.defaultBranch main
  git config --global --unset-all filter.xet.process || echo "global already unset"
  git config --global --unset-all filter.xet.required || echo "global already unset"


  if [[ -z $XET_TESTING_REMOTE ]] ; then
    if [[ -z $XET_CAS_SERVER ]] ; then
      export XET_CAS_SERVER="local://$PWD/cas"
      mkdir -p "$PWD/cas"
    fi
  fi

}

# Called from each test; runs tests against the rest of the things.
setup_basic_run_environment() {

  setup_isolated_environment

}

die() { 
  >&2 echo "ERROR:>>>>> $1 <<<<<"
  exit 1
}
export -f die
  
# support both Mac OS and Linux for these scripts
if hash md5 2>/dev/null; then 
    checksum() {
        md5 -q $1
    }
    checksum_string() {
        echo $1 | md5 -q
    }
else
    checksum() {
        md5sum $1 | head -c 32
    }
    checksum_string() {
        echo $1 | md5sum
    }
fi

export -f checksum
export -f checksum_string

create_bare_repo() {
  # Clean up the remote repo.
  if [[ ! -z $XET_TESTING_REMOTE ]] ; then 
    # Reset the remote branch main to a single initial commit
    >&2 rm -rf origin_blanch && mkdir origin_blank && cd origin_blank
    >&2 git init 
    >&2 git xet init --local --force
    >&2 git remote add origin $XET_TESTING_REMOTE
    >&2 git fetch --all 
    >&2 git push --force origin main 

    # Delete all other branches
    >&2 remotes_to_del=$(git branch -r -l --format '%(refname)' | sed 's|refs/remotes/origin/||' | grep -v HEAD | grep -v main | grep -v notes)

    for branch in $remotes_to_del ; do 
      >&2 echo "Deleting remote branch $branch on remote."
      >&2 git push origin --delete $branch
    done
    
    >&2 git clone $XET_TESTING_REMOTE origin_tmp
    >&2 pushd origin_tmp
    

    >&2 popd
    echo $XET_TESTING_REMOTE
  else 
    >&2 repo=origin
    >&2 rm -rf $repo
    >&2 mkdir -p $repo 
    >&2 pushd $repo
    >&2 git init --bare --initial-branch=main
    >&2 popd
    
    echo $PWD/$repo
  fi
}
export -f create_bare_repo 

create_bare_xet_repo() {
  # Clean up the remote repo.
  if [[ ! -z $XET_TESTING_REMOTE ]] ; then 
    create_bare_repo $@
  else 
    >&2 repo=origin
    >&2 rm -rf $repo
    >&2 mkdir -p $repo 
    >&2 pushd $repo
    >&2 git init --bare --initial-branch=main
    >&2 git xet init
    >&2 popd
    
    echo $PWD/$repo
  fi
}
export -f create_bare_xet_repo 

create_data_file() {
  f="$1"
  len=$2

  printf '\xff' > $f # Start with this to ensure utf-8 encoding fails quickly.
  cat /dev/random | head -c $(($2 - 1)) >> $f
  echo $(checksum $f)
}
export -f create_data_file 

append_data_file() {
  f="$1"
  len=$2

  printf '\xff' >> $f # Start with this to ensure utf-8 encoding fails quickly.
  cat /dev/random | head -c $(($2 - 1)) >> $f
  echo $(checksum $f)
}
export -f append_data_file

assert_files_equal() {
  # Use fastest way to determine content equality.
  cmp --silent $1 $2 || die "Assert Failed: Files $1 and $2 not equal."
}
export -f assert_files_equal

assert_files_not_equal() {
  # Use fastest way to determine content equality.
  cmp --silent $1 $2 && die "Assert Failed: Files $1 and $2 should not be equal." || >&2 echo "Files $1 and $2 not equal."
}
export -f assert_files_not_equal

assert_stored_as_pointer_file() {
  set -e
  file=$1
  match=$(git show HEAD:$file | head -n 1 | grep -F '# xet version' || echo "")
  [[ !  -z "$match" ]] || die "File $file does not appear to be stored as a pointer file."
}
export -f assert_stored_as_pointer_file

assert_stored_as_full_file() {
  set -e
  file=$1
  match=$(git show HEAD:$file | head -n 1 | grep -F '# xet version' || echo "")
  [[ -z "$match" ]] || die "File $file does not appear to be stored as a pointer file."
}
export -f assert_stored_as_full_file

assert_is_pointer_file() {
  set -e
  file=$1
  match=$(cat $file | head -n 1 | grep -F '# xet version' || echo "")
  [[ !  -z "$match" ]] || die "File $file does not appear to be a pointer file."
}
export -f assert_is_pointer_file

assert_pointer_file_size() {
  set -e
  file=$1
  size=$2

  assert_is_pointer_file $file

  filesize=$(cat $file | grep -F filesize | sed -E 's|.*filesize = ([0-9]+).*|\1|' || echo "")
  [[ $filesize == $size ]] || die "Pointer file $file gives incorrect size; $filesize, expected $size."
}
export -f assert_pointer_file_size

pseudorandom_stream() {
  key=$1

  while true ; do
    key=$(checksum_string $key)
    echo "$(echo $key | xxd -r -p)" 2>/dev/null || exit 0
  done 
}
export -f pseudorandom_stream 

write_file_checksum() { 
  f="$1"
  f_hash=$f.hash

  checksum $f > $f_hash
}

export -f write_file_checksum

check_file_checksum() { 
  f="$1"
  f_hash=$f.hash
  
  if [[ ! -e $f ]] ; then 
    die "File $f does not exist."
  fi
  
  if [[ ! -e $f_hash ]] ; then 
    die "File $f exists, but the checksum hash file $f_hash does not exist."
  fi

  h1=$(checksum $1)
  h2=$(cat $f_hash)

  [[ $h1 == $h2 ]] || die "Assert Failed: File $1 does not match its checksum."
}
export -f check_file_checksum 

create_csv_file() { 
  csv_file="$1"
  key="$2"
  n_lines="$3"
  n_repeats="${4:-1}"
  n_lines_p_1=$((n_lines + 1))

  pseudorandom_stream "$key" | hexdump -v -e '5/1 "%02x""\n"' |
    awk -v OFS='\t' 'NR == 1 { print "foo", "bar", "baz" }
    { print "S"substr($0, 1, 4), substr($0, 5, 2), substr($0, 7, 2)"."substr($0, 9, 1), 6, 3}' \
    | head -n $((n_lines + 1)) | tr 'abcdef' '123456' > $csv_file.part     
    
  cat $csv_file.part > $csv_file

  for i in {0..n_repeats} ; do 
    cat $csv_file.part | tail -n $n_lines $csv_file.part >> $csv_file
  done

  rm $csv_file.part
}
export -f create_csv_file

create_random_csv_file() { 
  f="$1"
  n_lines="$2"
  n_repeats="${3:-1}"
  n_lines_p_1=$((n_lines + 1))

  cat /dev/random | hexdump -v -e '5/1 "%02x""\n"' |
    awk -v OFS='\t' 'NR == 1 { print "foo", "bar", "baz" }
    { print "S"substr($0, 1, 4), substr($0, 5, 2), substr($0, 7, 2)"."substr($0, 9, 1), 6, 3}' \
    | head -n $((n_lines + 1)) | tr 'abcdef' '123456' > $f.part     
    
  cat $f.part > $f

  for i in {0..n_repeats} ; do 
    cat $f.part | tail -n $n_lines $f.part >> $f
  done

  rm $f.part
}
export -f create_random_csv_file

create_text_file() { 
  text_file="$1"
  key="$2"
  n_lines="$3"
  n_repeats="${4:-1}"

  create_csv_file "$text_file.temp" "$key" "$n_lines" "$n_repeats"

  cat "$text_file.temp" | tr ',0123456789' 'ghijklmnopq' > $text_file
  rm "$text_file.temp"
}
export -f create_text_file

random_tag() {
  cat /dev/random | head -c 64 | checksum_string
}
export -f random_tag 

integrations_setup_environment() {
  # Set up the base environment. 
  setup_isolated_environment
  
  mkdir config_files/

  git config --global receive.denyCurrentBranch ignore
  git config --global push.autoSetupRemote true

  # Make sure the filter is configured globally.
  git xet install 

}
export -f integrations_setup_environment


integrations_new_bare_repo() {

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
export -f integrations_new_bare_repo

# Use the local versions of the 
integrations_new_repo() {
    local name="$1"
    local repo_dir="$base_dir/$name"

    rm -rf "$repo_dir"
    mkdir "$repo_dir"
    pushd "$repo_dir"

    git init

    popd
}
export -f integrations_new_repo

integrations_create_file_in_repo() {
    local name="$1"
    local repo_dir="$base_dir/$name"
    local file_name="$repo_dir/$2"

    pushd "$repo_dir"
    create_data_file "$file_name" 100000
    git add "$file_name"
    git commit -m "Added file $file_name"
    popd
}
export -f integrations_create_file_in_repo

integrations_init_repo_as_xet () { 
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
export -f integrations_init_repo_as_xet


integrations_new_tmp_repo () { 
    local tmp_repo="tmp_repo_$(random_tag)"
    >&2 integrations_new_repo ${tmp_repo}
    echo ${tmp_repo}
}
export -f integrations_new_tmp_repo

integrations_new_tmp_bare_repo () { 
    local tmp_repo="tmp_repo_$(random_tag)"
    >&2 integrations_new_bare_repo ${tmp_repo}
    echo ${tmp_repo}
}
export -f integrations_new_tmp_bare_repo

integrations_new_name() { 
    base=$1
    echo "$base_$(random_tag)"
}
export -f integrations_new_name

integrations_do_push() { 
    local repo_1="$1"
    local repo_dir_1="$base_dir/$repo_1"

    local repo_2="$2"
    local repo_dir_2="$base_dir/$repo_2"

    pushd "$repo_dir_1"
    git push --force "$repo_dir_2" 
    popd
}
export -f integrations_do_push

integrations_simulate_pr_merge() { 
    local repo_1="$1"
    local repo_dir_1="$base_dir/$repo_1"

    local repo_2="$2"
    local repo_dir_2="$base_dir/$repo_2"

    local branch=${3:-main}
    local remote_branch=${4:-main}

    # Create a temporary repo from which to do the merge. 
    tmp_repo=$(integrations_new_tmp_repo)
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
export -f integrations_simulate_pr_merge

# Create a new bare repo that simulates a fork of the original repo.
integrations_simulate_fork() {
    local repo_1="$1"
    local repo_dir_1="$base_dir/$repo_1"

    local repo_2="$2"
    local repo_dir_2="$base_dir/$repo_2"

    # Create a temporary new bare repo to push things to it.
    local tmp_repo=$(integrations_new_tmp_bare_repo)
    local tmp_repo_dir="$base_dir/$tmp_repo"

    integrations_do_push $repo_1 $tmp_repo 

    # Now go to the bare repo, which doesn't have the hooks or filter or anything, 
    # and simply push from that to the new repo.  This should drop all the refs and 
    # stuff.
    integrations_new_bare_repo $repo_2

    integrations_do_push $tmp_repo $repo_2 
}
export -f integrations_simulate_fork





