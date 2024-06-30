#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]:-$0}")" &>/dev/null && pwd 2>/dev/null)"
. "$SCRIPT_DIR/initialize.sh"

setup_testing_environment
setup_xetldfs "$LDPRELOAD_LIB"

git xet install

remote=$(create_bare_repo)

git clone $remote repo_1

# file larger than 100 bytes will be checked-in as pointer file
export XET_CAS_SIZETHRESHOLD=16
text_1="abcdefghijklmnopqrstuvwxyz"
text_2="0123456789"
text_ins_at_10="${text_1:0:9}${text_2}${text_1:20}"

text_1_len=$(echo -n $text_1 | wc -c)
text_2_len=$(echo -n $text_2 | wc -c)
text_12_len=$(echo -n "${text_1}${text_2}" | wc -c)

pushd repo_1
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

all_file_text=$(cat t*.txt)

git add .
git commit -m "add text data"
git push origin main
popd

# test truncate write into this pointer file and
# get the correct content.
git xet clone --lazy $remote repo_2

pushd repo_2
assert_is_pointer_file text_data.txt

for n in 1 2 3 4 ; do 
    assert_is_pointer_file t$n.txt
    assert_is_pointer_file m$n.txt
    assert_is_pointer_file l$n.txt
done

# Read
(
    xetfs_on
    [[ "$(x cat t1.txt)" == "$text_1" ]] || die "t1.txt not read as pointer." 
    [[ "$(x cat t*.txt)" == "$all_file_text" ]] || die "all text does not match correctly." 
    [[ "$(x cat_mmap m1.txt)" == "$text_1" ]] || die "m1.txt not read through mmap." 
    [[ "$(x cat_mmap m*.txt)" == "$all_file_text" ]] || die "all text does not match correctly with mmap." 

    # With linux
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        [[ "$(cat l1.txt)" == "$text_1" ]] || die "l1.txt not read as pointer." 
        [[ "$(cat l*.txt)" == "$all_file_text" ]] || die "all text does not match correctly." 
        [[ "$(bash -c 'cat l1.txt')" == "$text_1" ]] || die "m1.txt not read through bash cat." 
        [[ "$(bash -c 'cat l*.txt')" == "$all_file_text" ]] || die "all text does not match correctly in linux bash." 
        [[ "$(bash -c 'x cat_mmap l1.txt')" == "$text_1" ]] || die "l1.txt not read through bash cat." 
        [[ "$(bash -c 'x cat_mmap l*.txt')" == "$all_file_text" ]] || die "all text does not match correctly in linux bash." 
    fi
)

popd 

# Reset everything. 
git xet clone --lazy $remote repo_3
pushd repo_3

for n in 1 2 3 4 ; do 
    assert_is_pointer_file t$n.txt
    assert_is_pointer_file m$n.txt
    assert_is_pointer_file l$n.txt
done

# Overwrite regular. 
assert_is_pointer_file t2.txt
assert_is_pointer_file m2.txt
assert_is_pointer_file l2.txt


(
    xetfs_on
    echo $text_2 | x write t2.txt

    [[ $(cat t2.txt) == $text_2 ]] || die "rewrite pointer file failed"

    assert_file_size t2.txt $text_1_len 

    # Overwrite mmap 
    echo $text_2 | x write_mmap t2.txt
    [[ "$(x cat t2.txt)" == "$text_2" ]] || die "t2.txt not overwritten." 

    # Overwrite, linux specific
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        echo $text_2 > l2.txt
        [[ "$(cat l2.txt)" == "$text_2" ]] || die "l2.txt not overwritten." 
    fi
)

# Append
assert_is_pointer_file t3.txt
assert_is_pointer_file m3.txt
assert_is_pointer_file l3.txt

(
    xetfs_on
    # Regular
    echo $text_2 | x append t3.txt
    [[ $(cat t3.txt) == "${text_1}${text_2}" ]] || die "append to t3.txt failed"

    # Append, mmap
    echo $text_2 | x append_mmap m3.txt
    [[ $(cat m3.txt) == "${text_1}${text_2}" ]] || die "append to t3.txt with mmap failed" 

    # Append, linux specific
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        echo $text_2 >> l3.txt
        [[ $(cat l3.txt) == "${text_1}${text_2}" ]] || die "append to l3.txt with mmap failed" 
    fi
)

# Write at specific location
assert_is_pointer_file t4.txt
assert_is_pointer_file m4.txt
assert_is_pointer_file l4.txt

(
    xetfs_on
    echo $text_2 | x write_at 10 t4.txt
    [[ $(cat t4.txt) == "${text_ins_at_10}" ]] || die "write at to t4.txt failed" 

    # With MMap
    echo $text_2 | x write_at_mmap 10 m4.txt
    [[ $(cat m4.txt) == "${text_ins_at_10}" ]] || die "write at to t4.txt failed" 

    # Linux specific... Not sure how to do this currently without write redirection.
)

popd

# Finally, write to all the files at once. 
git xet clone --lazy $remote repo_4
pushd repo_4

for n in 1 2 3 4 ; do 
    assert_is_pointer_file t$n.txt
    assert_is_pointer_file m$n.txt
    assert_is_pointer_file l$n.txt
done

(
    xetfs_on
    
    # Regular write to all the files
    echo $text_2 | x write t*.txt

    for n in 1 2 3 4 ; do 
        [[ $(cat t$n.txt) == "${text_1}" ]] || die "Bulk write failed."
    done

    # MMap write to all the files at once
    echo $text_2 | x write_mmap m*.txt

    for n in 1 2 3 4 ; do 
        [[ $(cat m$n.txt) == "${text_1}" ]] || die "Bulk write failed."
    done
)