#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]:-$0}")" &>/dev/null && pwd 2>/dev/null)"
. "$SCRIPT_DIR/initialize.sh"

statfuncs=$(echo "#include <sys/stat.h>" | gcc -xc - -E -dD | grep stat)
die $statfuncs

# This gives us $remote, $text_1, $text_1_len, $text_2, $text_2_len, $all_file_text, etc. 
# It also sets XET_CAS_SIZETHRESHOLD such that $text_1 is stored in CAS and as a pointer file.
#
# Repo from in $remote has 12 files, t1.txt, t2.txt, t3.txt, t4.txt, and same for l and m instead of t.
# All are stored as pointer files and resolve in content to $text_1.
. "$SCRIPT_DIR/setup_test_repo.sh"

git xet clone --lazy $remote repo

pushd repo
assert_is_pointer_file text_data.txt

for n in 1 2 3 4 ; do 
    assert_is_pointer_file t$n.txt
    verify_size t$n.txt $text_1_len 

    assert_is_pointer_file m$n.txt
    verify_size m$n.txt $text_1_len 

    assert_is_pointer_file l$n.txt
    verify_size l$n.txt $text_1_len 
done

# Read
(
    xetfs_on
    [[ "$(x cat t1.txt)" == "$text_1" ]] || die "t1.txt not read as pointer." 
    [[ "$(x cat t?.txt)" == "$all_file_text" ]] || die "all text does not match correctly." 
    [[ "$(x cat-mmap m1.txt)" == "$text_1" ]] || die "m1.txt not read through mmap." 
    [[ "$(x cat-mmap m?.txt)" == "$all_file_text" ]] || die "all text does not match correctly with mmap." 

    # With linux
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        [[ "$(bash -c 'x cat t1.txt')" == "$text_1" ]] || die "t1.txt not read as pointer."
        [[ "$(bash -c 'x cat t?.txt')" == "$all_file_text" ]] || die "all text does not match correctly."
        [[ "$(bash -c 'x cat-mmap m1.txt')" == "$text_1" ]] || die "m1.txt not read through mmap."
        [[ "$(bash -c 'x cat-mmap m?.txt')" == "$all_file_text" ]] || die "all text does not match correctly with mmap."

        [[ "$(cat l1.txt)" == "$text_1" ]] || die "l1.txt not read as pointer." 
        [[ "$(cat l*.txt)" == "$all_file_text" ]] || die "all text does not match correctly." 
        [[ "$(bash -c 'cat l1.txt')" == "$text_1" ]] || die "m1.txt not read through bash cat." 
        [[ "$(bash -c 'cat l*.txt')" == "$all_file_text" ]] || die "all text does not match correctly in linux bash." 
        [[ "$(bash -c 'x cat-mmap l1.txt')" == "$text_1" ]] || die "l1.txt not read through bash cat." 
        [[ "$(bash -c 'x cat-mmap l?.txt')" == "$all_file_text" ]] || die "all text does not match correctly in linux bash." 
    
        # Ensure that things work when the library is loaded in bash.
        [[ "$(bash -c 'cat t1.txt > /dev/null ; x cat t1.txt')" == "$text_1" ]] || die "t1.txt not read as pointer." 
        [[ "$(bash -c 'cat t?.txt > /dev/null ; x cat t?.txt')" == "$all_file_text" ]] || die "all text does not match correctly." 
        [[ "$(bash -c 'cat m1.txt > /dev/null ; x cat-mmap m1.txt')" == "$text_1" ]] || die "m1.txt not read through mmap." 
        [[ "$(bash -c 'cat m?.txt > /dev/null ; x cat-mmap m?.txt')" == "$all_file_text" ]] || die "all text does not match correctly with mmap." 
    fi
)

popd 