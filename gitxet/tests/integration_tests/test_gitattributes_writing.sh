#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"
setup_basic_run_environment

git xet install

mkdir repo
cd repo
git init
git xet init --force


mv .gitattributes .gitattributes.bk
echo "*foobar filter=" > .gitattributes
cat .gitattributes.bk >> .gitattributes

git add .gitattributes
git commit -m "Dummy .gitattributes file."

echo "data" > data.dat

hash_1=$(md5sum .gitattributes)

out_text=$(git add data.dat 2>&1)

if [[ ! "$out_text" == *"WARN"*"gitattributes"*"git xet init"* ]] ; then 
   die "git xet add did not correctly tell user to run git xet init (out = $out_text)"
fi

if [[ ! $hash_1 = $(md5sum .gitattributes) ]] ; then
  die "filter process modified .gitattributes."
fi

git commit -a -m "Added data.dat."
git xet init --force

hash_2=$(md5sum .gitattributes)

if [[ $hash_1 == $hash_2 ]] ; then 
  die "git xet init did not update .gitattributes file"
fi

mv .gitattributes .gitattributes.bk
echo "# XET LOCK" > .gitattributes
echo "*foobar filter=" >> .gitattributes
cat .gitattributes.bk >> .gitattributes

git add .gitattributes
git commit -m "Dummy .gitattributes file #2."


hash_3=$(md5sum .gitattributes)

out_text=$(git add data.dat 2>&1)

if [[ ! $hash_3 = $(md5sum .gitattributes) ]] ; then
  die "filter process modified .gitattributes."
fi
  
out_text=$(git xet init --force 2>&1)
if [[ ! "$out_text" == *"ERROR"*"gitattributes"*"XET LOCK"* ]] ; then 
  die "git xet init did not report lock."
fi

if [[ ! $hash_3 = $(md5sum .gitattributes) ]] ; then
  die "git xet init modified .gitattributes when lock present."
fi






