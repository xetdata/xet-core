# Set up local, self-contained config stuff to make sure the environment for the tests is hermetic.
export GIT_CONFIG_GLOBAL="$PWD/.gitconfig"

# This is needed as older versions of git only go to $HOME/.gitconfig and do not respect
# the GIT_CONFIG_GLOBAL environment variable.  
export HOME="$PWD"

[[ -d "$HOME/cas/" ]] || die "CAS directory $HOME/cas/ does not exist."

# Set the CAS server to the local directory.
export XET_CAS_SERVER="local://$PWD/cas/"

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

die() { 
  >&2 echo "ERROR:>>>>> $1 <<<<<"
  exit 1
}

export -f die

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

create_bare_repo() {
    >&2 repo=origin
    >&2 rm -rf $repo
    >&2 mkdir -p $repo 
    >&2 pushd $repo
    >&2 git init --bare --initial-branch=main
    >&2 popd
    
    echo $PWD/$repo
}
export -f create_bare_repo 

pseudorandom_stream() {
  key=$1

  while true ; do
    key=$(checksum_string $key)
    echo "$(echo $key | xxd -r -p)" 2>/dev/null || exit 0
  done 
}
export -f pseudorandom_stream 

create_data_file() {
  f="$1"
  key="$2"
  repeating=$(($3 / 100))
  s=$(pseudorandom_stream $key | head -c 100)
  
  printf '\xff' > $f # Start with this to ensure utf-8 encoding fails quickly.
  
  # Repeat this k times.  This allows for efficient compression
  for k in `seq $repeating` ; do echo "$s" >> $f ; done
}
export -f create_data_file

create_random_data_file() {
  f="$1"
  printf '\xff' > $f # Start with this to ensure utf-8 encoding fails quickly.
  cat /dev/random | head -c $2 >> $f
}
export -f create_random_data_file

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

assert_files_equal() {

  h1=$(checksum $1)
  h2=$(checksum $2)
  [[ $h1 == $h2 ]] || die "Assert Failed: Files $1 and $2 not equal."
}
export -f assert_files_equal

assert_files_not_equal() {

  h1=$(checksum $1)
  h2=$(checksum $2)
  [[ $h1 != $h2 ]] || die "Assert Failed: Files $1 and $2 should not be equal."
}
export -f assert_files_not_equal

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


