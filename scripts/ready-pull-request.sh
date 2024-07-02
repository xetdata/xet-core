#!/bin/bash -e 

base_dir="${PWD}"

# Find all subdirectories containing a Cargo.toml file
for d in * ; do 
  if [[ ! -e "$d/Cargo.toml" ]] ; then
    continue
  fi
  
  cd "$base_dir"

  cd $d

  # Run cargo fmt
  if ! cargo fmt > /dev/null 2>&1; then
    echo "cargo fmt failed in $(pwd)" >&2
  fi

  # Run cargo generate-lockfile
  if ! cargo generate-lockfile > /dev/null 2>&1; then
    echo "cargo generate-lockfile failed in $(pwd)" >&2
  fi

done 

# Always return true to allow the commit to proceed
exit 0

