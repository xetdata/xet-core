#!/usr/bin/env bash

if [[ $# != 1 ]] ; then 
   >&2 echo "Usage: <input archive>"
>&2 echo "
Input archive must unpack into a single directory matching the name of the archive, 
with that directory having two subdirectories, repo/ and cas/, and a single scripts.txt 
file that lists the names of the scripts to run.

The repo/ subdirectory contains a snapshot of the repo to be tested against.

The scripts/ subdirectory is a collection of test_XXX.sh bash test scripts.  All of the 
scripts in this subdirectory matching test_*.sh will be run *in* the repo/ directory.  
A non-zero exit status will indicate a test failure.  Output that matches the pattern
'ERROR:>>>>>(.*)<<<<<' will be surfaced to rust as an error message.

The cas/ subdirectory will be the local cas; when creating the repo, the option 
XET_CAS_SERVER=local://<directory> should be used to capture the CAS. While running 
any of the above scirpts, this variable will be set to the cas/ subdirectory. "

   exit 1
fi

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";

die() { 
  >&2 echo "ERROR:>>>>> $1 <<<<<"
  exit 1
}
abspath() { echo "$(cd "$(dirname $1)"; pwd)"/"$(basename $1)"; }

input_archive="$(abspath $1)"
archive_name="${input_archive##*/}"
base_name="${archive_name%.tar.bz2}"
>&2 echo "Archive base name = $base_name"

dest_dir="$(abspath .)"
archive_name="${filename##*/}"
repo_unpack_dir="$dest_dir/$base_name"

>&2 echo "Unpacking $input_archive."
tar xf $input_archive

[[ -d $repo_unpack_dir ]] || die "Expected input archive to unpack into directory $base_name/; directory not found. ls = $(ls -A $(dirname $repo_unpack_dir))"

cas_dir=$repo_unpack_dir/cas
repo_dir=$repo_unpack_dir/repo

[[ -e $cas_dir && -e $repo_dir ]] || die "Input archive must unpack into two subdirectories, $base_name/cas and $base_name/repo."

scripts_list=$repo_unpack_dir/scripts.txt
[[ -e $scripts_list ]] || die "Expected list of scripts to run in $scripts_list." 

cd $repo_unpack_dir

for script in $(cat $scripts_list) ; do 
  set -e
  bash -e $SCRIPT_DIR/$script
done






