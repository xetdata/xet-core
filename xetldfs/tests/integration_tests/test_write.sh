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

text_1_len=$(echo -n $text_1 | wc -c)
text_2_len=$(echo -n $text_2 | wc -c)
text_12_len=$(echo -n "${text_1}${text_2}" | wc -c)

pushd repo_1
[[ $(git branch) == *"main"* ]] || git checkout -b main
git xet init --force
git push origin main # This created a commit, so push it to main.

echo -n $text_1 > text_data.txt
text_data_file_size=$(file_size text_data.txt)

for n in 1 2 3 4 5 6 7 8 ; do 
    cp text_data.txt t$n.txt 
done

git add .
git commit -m "add text data"
git push origin main
popd

# test truncate write into this pointer file and
# get the correct content.
git xet clone --lazy $remote repo_2

pushd repo_2
assert_is_pointer_file text_data.txt
assert_is_pointer_file t1.txt

xetfs_on
echo $text_2 | x write t1.txt
xetfs_off

[[ $(cat t1.txt) == $text_2 ]] || die "rewrite pointer file failed"

assert_file_size t1.txt $text_1_len 

assert_is_pointer_file t2.txt

xetfs_on
echo $text_2 | x append t2.txt
xetfs_off

[[ $(cat t2.txt) == "${text_1}${text_2}" ]] || die "append to pointer file failed"

assert_file_size t2.txt $text_2_len





if [[ "$OSTYPE" == "linux-gnu"* ]]; then

    xetfs_on 
    bash -c "echo -n 'some10char' > .txt"



elif [[ "$OSTYPE" == "darwin"* ]]; then
fi

[[ $(cat text_data.txt) == "some10char" ]] || die "rewrite pointer file failed"

popd

# test append write into this pointer file and
# get the correct content.
git xet clone --lazy $remote repo_3

pushd repo_3
assert_is_pointer_file text_data.txt
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    LD_PRELOAD=$LDPRELOAD_LIB bash -c "echo -n 'some10char' >> text_data.txt"
elif [[ "$OSTYPE" == "darwin"* ]]; then
    DYLD_INSERT_LIBRARIES=$LDPRELOAD_LIB echo -n "some10char" | x append text_data.txt
fi

file_size_after_write=$(file_size text_data.txt)
[[ $((file_size_after_write - 10)) -eq $text_data_file_size ]] || die "append write pointer file didn't materialize first"
# test last 10 characters are "some10char"
[[ $(cat text_data.txt | tail -c 10) == "some10char" ]] || die "append write pointer file failed"

# test text before the last 10 characters are the original contents
dd if=text_data.txt of=original_text_data.txt bs=$text_data_file_size count=1
assert_files_equal original_text_data.txt ../repo_1/text_data.txt

popd

# test write at any position into this pointer file and
# get the correct content.
git xet clone --lazy $remote repo_4

pushd repo_4
assert_is_pointer_file text_data.txt

xetfs_on
echo -n "some10char" | x writeat 100 text_data.txt
xetfs_off

file_size_after_write=$(x fstat text_data.txt)

[[ $file_size_after_write -eq $text_data_file_size ]] || die "file size changed after seek and write"
# test the characters in [100, 110) are "some10char"
[[ $(cat text_data.txt | head -c 110 | tail -c 10) == "some10char" ]] || die "seek and write pointer file failed"
# test characters in [0, 100) are identical
[[ $(cat text_data.txt | head -c 100) == $(cat ../repo_1/text_data.txt | head -c 100) ]] || die "seek and write pointer file didn't materialize first"
# test characters in [110, 150) are identical
[[ $(cat text_data.txt | head -c 150 | tail -c 40) == $(cat ../repo_1/text_data.txt | head -c 150 | tail -c 40) ]] || die "seek and write pointer file corrupted materialization"




if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo -n "some10char" | dd of=text_data.txt bs=1 seek=100 conv=notrunc # overwrite at pos 100
fi



popd
