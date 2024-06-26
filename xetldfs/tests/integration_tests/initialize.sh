#!/usr/bin/env bash

export XET_LOG_FORMAT=compact
export XET_DISABLE_VERSION_CHECK="1"

# Set up logging
export XET_PRINT_LOG_FILE_PATH=1
export XET_LOG_PATH="$PWD/logs/log_{timestamp}_{pid}.txt"

# Workaround for git reference transaction hook issues
export GIT_CLONE_PROTECTION_ACTIVE=false

# With these, Log the filename, function name, and line number when showing where we're executing.
set -o xtrace
export PS4='+($(basename ${BASH_SOURCE}):${LINENO}): ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'

setup_isolated_environment() {
  set +x

  # Set up local, self-contained config stuff to make sure the environment for the tests is hermetic.
  export GIT_CONFIG_GLOBAL="$PWD/.gitconfig"

  # This is needed as older versions of git only go to $HOME/.gitconfig and do not respect
  # the GIT_CONFIG_GLOBAL environment variable.
  export HOME=$PWD
  export base_dir="$HOME"

  if [[ ! -e $GIT_CONFIG_GLOBAL ]]; then
    echo "[user]
        name = Xet Tester
        email = test@xetdata.com
    " >$GIT_CONFIG_GLOBAL
  else
    die "These tests may overwrite global settings; please run \
  them in a directory without a .gitconfig present."
  fi

  # Do some checks to make sure that everything is set up
  username=$(git config --get user.name || echo "")
  [[ ! -z $username ]] || die "Git config user.name not set."

  useremail=$(git config --get user.email || echo "")
  [[ ! -z $useremail ]] || die "Git config user.email not set."

  git config --global init.defaultBranch main
  git config --global --unset-all filter.xet.process || echo "global already unset"
  git config --global --unset-all filter.xet.required || echo "global already unset"

  if [[ -z $XET_TESTING_REMOTE ]]; then
    if [[ -z $XET_CAS_SERVER ]]; then
      # In Cygwin or msys emulators, $PWD is returned in unix format. Directly
      # exporting XET_CAS_SERVER using this path format will crash git-xet because
      # a Windows build cannot understand such a path.
      # We convert it to Windows format using cygpath.
      local pwd=$PWD
      if [[ "$OSTYPE" == "cygwin" || "$OSTYPE" == "msys" ]]; then
        pwd=$(cygpath -wa $pwd)
      fi
      export XET_CAS_SERVER="local://$pwd/cas"
      mkdir -p "$PWD/cas"
    fi
  fi
  set -x
}

# Called from each test; runs tests against the rest of the things.
setup_basic_run_environment() {

  setup_isolated_environment

}

die() {
  echo >&2 "ERROR:>>>>> $1 <<<<<"
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
    echo $1 | md5sum | head -c 32
  }
fi

export -f checksum
export -f checksum_string

create_bare_repo() {
  set +x
  # Clean up the remote repo.
  if [[ ! -z $XET_TESTING_REMOTE ]]; then
    # Reset the remote branch main to a single initial commit
    rm >&2 -rf origin_blanch && mkdir origin_blank && cd origin_blank
    git >&2 init
    git >&2 xet init --local --force
    git >&2 remote add origin $XET_TESTING_REMOTE
    git >&2 fetch --all
    git >&2 push --force origin main

    # Delete all other branches
    remotes_to_del=$(git branch -r -l --format '%(refname)' | sed 's|refs/remotes/origin/||' | grep -v HEAD | grep -v main | grep -v notes) >&2

    for branch in $remotes_to_del; do
      echo >&2 "Deleting remote branch $branch on remote."
      git >&2 push origin --delete $branch
    done

    git >&2 clone $XET_TESTING_REMOTE origin_tmp
    pushd >&2 origin_tmp

    popd >&2
    echo $XET_TESTING_REMOTE
  else
    repo=origin >&2
    rm >&2 -rf $repo
    mkdir >&2 -p $repo
    pushd >&2 $repo
    git >&2 init --bare --initial-branch=main
    popd >&2

    echo $PWD/$repo
  fi
  set -x
}
export -f create_bare_repo

create_bare_xet_repo() {

  set +x

  # Clean up the remote repo.
  if [[ ! -z $XET_TESTING_REMOTE ]]; then
    create_bare_repo $@
  else
    repo=origin >&2
    rm >&2 -rf $repo
    mkdir >&2 -p $repo
    pushd >&2 $repo
    git >&2 init --bare --initial-branch=main
    git >&2 xet init
    popd >&2

    echo $PWD/$repo
  fi
}
export -f create_bare_xet_repo

create_data_file() {
  set +x
  
  f="$1"
  len=$2

  printf '\xff' >$f # Start with this to ensure utf-8 encoding fails quickly.
  cat /dev/random | head -c $(($2 - 1)) >>$f
  echo $(checksum $f)
  set -x
}
export -f create_data_file

append_data_file() {
  f="$1"
  len=$2

  printf '\xff' >>$f # Start with this to ensure utf-8 encoding fails quickly.
  cat /dev/random | head -c $(($2 - 1)) >>$f
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
  cmp --silent $1 $2 && die "Assert Failed: Files $1 and $2 should not be equal." || echo >&2 "Files $1 and $2 not equal."
}
export -f assert_files_not_equal

assert_stored_as_pointer_file() {
  set -e
  file=$1
  match=$(git show HEAD:$file | head -n 1 | grep -F '# xet version' || echo "")
  [[ ! -z "$match" ]] || die "File $file does not appear to be stored as a pointer file."
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
  [[ ! -z "$match" ]] || die "File $file does not appear to be a pointer file."
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

  while true; do
    key=$(checksum_string $key)
    echo "$(echo $key | xxd -r -p)" 2>/dev/null || exit 0
  done
}
export -f pseudorandom_stream

create_csv_file() {
  set +x
  csv_file="$1"
  key="$2"
  n_lines="$3"
  n_repeats="${4:-1}"
  n_lines_p_1=$((n_lines + 1))

  pseudorandom_stream "$key" | hexdump -v -e '5/1 "%02x""\n"' |
    awk -v OFS='\t' 'NR == 1 { print "foo", "bar", "baz" }
    { print "S"substr($0, 1, 4), substr($0, 5, 2), substr($0, 7, 2)"."substr($0, 9, 1), 6, 3}' |
    head -n $((n_lines + 1)) | tr 'abcdef' '123456' >$csv_file.part

  cat $csv_file.part >$csv_file

  for i in {0..n_repeats}; do
    tail -n $n_lines $csv_file.part >>$csv_file
  done

  rm $csv_file.part
  set -x

}
export -f create_csv_file

create_random_csv_file() {
  set +x
  
  f="$1"
  n_lines="$2"
  n_repeats="${3:-1}"
  n_lines_p_1=$((n_lines + 1))

  cat /dev/random | hexdump -v -e '5/1 "%02x""\n"' |
    awk -v OFS='\t' 'NR == 1 { print "foo", "bar", "baz" }
    { print "S"substr($0, 1, 4), substr($0, 5, 2), substr($0, 7, 2)"."substr($0, 9, 1), 6, 3}' |
    head -n $((n_lines + 1)) | tr 'abcdef' '123456' >$f.part

  cat $f.part >$f

  for i in {0..n_repeats}; do
    tail -n $n_lines $f.part >>$f
  done

  rm $f.part
  set -x
}
export -f create_random_csv_file

create_text_file() {
  set +x

  text_file="$1"
  key="$2"
  n_lines="$3"
  n_repeats="${4:-1}"

  create_csv_file "$text_file.temp" "$key" "$n_lines" "$n_repeats"

  cat "$text_file.temp" | tr ',0123456789' 'ghijklmnopq' >$text_file
  rm "$text_file.temp"
  set -x
}
export -f create_text_file

random_tag() {
  cat /dev/random | head -c 64 | checksum_string
}
export -f random_tag

file_size() {
  if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    stat --printf="%s" $1
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    stat -f%z $1
  fi
}
