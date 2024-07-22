#!/usr/bin/env bash

# Sourcing this script, after initialize.sh, gives us $remote, $text_1, $text_1_len, $text_2, 
# $text_2_len, $all_file_text, etc. 
#
# It also sets XET_CAS_SIZETHRESHOLD such that $text_1 is stored in CAS and as a pointer file.
#
# Repo from in $remote has 12 files, t1.txt, t2.txt, t3.txt, t4.txt, and same for l and m instead of t.
# All are stored as pointer files and resolve in content to $text_1.
# 
# Calling verify_size checks the size of things too.  

setup_xetldfs_testing_env 

git xet install

remote=$(create_bare_repo)

git clone $remote repo_setup

# file larger than 16 bytes will be checked-in as pointer file
export XET_CAS_SIZETHRESHOLD=16
text_1="abcdefghijklmnopqrstuvwxyz"
text_2="0123456789"
text_ins_at_10="${text_1:0:10}${text_2}${text_1:20}"

text_1_len=$(echo -n $text_1 | wc -c)
text_2_len=$(echo -n $text_2 | wc -c)
text_12_len=$(echo -n "${text_1}${text_2}" | wc -c)


pushd repo_setup
[[ $(git branch) == *"main"* ]] || git checkout -b main
git xet init --force
git push origin main # This created a commit, so push it to main.

echo -n $text_1 > text_data.txt
text_data_file_size=$(file_size text_data.txt)

for n in 1 2 3 4 ; do 
    cp text_data.txt t$n.txt 
    cp text_data.txt m$n.txt 
    cp text_data.txt l$n.txt 
done

all_file_text="$(cat t?.txt)"

git add .
git commit -m "add text data"
git push origin main
popd

# Some helper functions

verify_size() {
    file=$1
    expected_len=$2
    (
        xetfs_on

        >&2 echo "STAT STRACE:"
        strace x stat $file
        >&2 echo "STAT STRACE complete:"

        len=$(x stat $file)
        [[ $len == $expected_len ]] || die "x stat length of $file is wrong; got $len, expected $expected_len"

        len=$(x fstat $file)
        [[ $len == $expected_len ]] || die "x fstat length of $file is wrong; got $len, expected $expected_len"

        len=$(x seek-tell $file)
        [[ $len == $expected_len ]] || die "x seek-tell length of $file is wrong; got $len, expected $expected_len"

        if [[ "$OSTYPE" == "linux-gnu"* ]]; then
            len=$(stat --printf="%s" $file)
            [[ $len == $expected_len ]] || die "linux stat length of $file is wrong; got $len, expected $expected_len"
        fi
    )
}