#!/usr/bin/env bash

# Set up local, self-contained config stuff to make sure the environment for the tests is hermetic.

export GIT_CONFIG_GLOBAL="$PWD/.gitconfig"
export XET_DISABLE_VERSION_CHECK="1"

# This is needed as older versions of git only go to $HOME/.gitconfig and do not respect
# the GIT_CONFIG_GLOBAL environment variable.  
export HOME=$PWD

# Set up logging
export XET_LOG_LEVEL=debug
export XET_LOG_FORMAT=compact
export XET_LOG_PATH="$HOME/logs/log_{timestamp}_{pid}.txt"
export XET_PRINT_LOG_FILE_PATH=1

# support both Mac OS and Linux for these scripts
if hash md5 2>/dev/null; then 
    checksum() {
        md5 -q $1
    }
else
    checksum() {
        md5sum $1 | head -c 32
    }
fi

die() { 
  >&2 echo "ERROR:>>>>> $1 <<<<<"
  exit 1
}

if [[ ! -e $GIT_CONFIG_GLOBAL ]] ; then
  echo "[user]
      name = Xet Tester
      email = test@xetdata.com
  " > $GIT_CONFIG_GLOBAL
else 
  die "These tests may overwrite global settings; please run \
them in a directory without a .gitconfig present."
fi

git config --global init.defaultBranch main
git config --global --unset-all filter.xet.process || echo "global already unset"
git config --global --unset-all filter.xet.required || echo "global already unset"

username=$(git config --get user.name || echo "")
[[ ! -z $username ]] || die "Git config user.name not set."

useremail=$(git config --get user.email || echo "")
[[ ! -z $useremail  ]] || die "Git config user.email not set."


# TODO: Uncomment this:
if [[ -z $XET_TESTING_REMOTE ]] ; then  
  if [[ -z $XET_CAS_SERVER ]] ; then 
    export XET_CAS_SERVER="local://$PWD/cas"
    mkdir -p "$PWD/cas"
  fi
fi

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

create_data_file() {
  f="$1"
  len=$2

  printf '\xff' > $f # Start with this to ensure utf-8 encoding fails quickly.
  cat /dev/random | head -c $(($2 - 1)) >> $f
  echo $(checksum $f)
}

append_data_file() {
  f="$1"
  len=$2

  printf '\xff' >> $f # Start with this to ensure utf-8 encoding fails quickly.
  cat /dev/random | head -c $(($2 - 1)) >> $f
  echo $(checksum $f)
}

assert_files_equal() {

  h1=$(checksum $1)
  h2=$(checksum $2)
  [[ $h1 == $h2 ]] || die "Assert Failed: Files $1 and $2 not equal."
}

assert_files_not_equal() {

  h1=$(checksum $1)
  h2=$(checksum $2)
  [[ $h1 != $h2 ]] || die "Assert Failed: Files $1 and $2 should not be equal."
}

assert_is_pointer_file() {
  set -e
  file=$1
  match=$(cat $file | head -n 1 | grep -F '# xet version' || echo "")
  [[ !  -z "$match" ]] || die "File $file does not appear to be a pointer file."
}

assert_pointer_file_size() {
  set -e
  file=$1
  size=$2

  assert_is_pointer_file $file

  filesize=$(cat $file | grep -F filesize | sed -E 's|.*filesize = ([0-9]+).*|\1|' || echo "")
  [[ $filesize == $size ]] || die "Pointer file $file gives incorrect size; $filesize, expected $size."
}







