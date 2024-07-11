#!/usr/bin/env bash
set -e
set -x

statfuncs=$(echo "#include <sys/stat.h>" | gcc -xc - -E -dD | grep stat)
echo $statfuncs >&2

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]:-$0}")" &>/dev/null && pwd 2>/dev/null)"
. "$SCRIPT_DIR/initialize.sh"

# This gives us $remote, $text_1, $text_1_len, $text_2, $text_2_len, $all_file_text, etc. 
# It also sets XET_CAS_SIZETHRESHOLD such that $text_1 is stored in CAS and as a pointer file.
#
# Repo from in $remote has 12 files, t1.txt, t2.txt, t3.txt, t4.txt, and same for l and m instead of t.
# All are stored as pointer files and resolve in content to $text_1.
. "$SCRIPT_DIR/setup_test_repo.sh"

git xet clone --lazy $remote repo_3
pushd repo_3

for n in 1 2 3 4 ; do 
    assert_is_pointer_file t$n.txt
    assert_is_pointer_file m$n.txt
    assert_is_pointer_file l$n.txt
done

(
    xetfs_on
    [[ $(x cat t2.txt) == $text_1 ]] || die "Issue getting t2.txt."
    
    verify_size t2.txt $text_1_len 
    echo -n $text_2 | x write t2.txt

    [[ $(x cat t2.txt) == $text_2 ]] || die "rewrite pointer file failed"

    verify_size t2.txt $text_2_len 

    # Overwrite mmap 
    echo -n $text_2 | x write-mmap m2.txt
    [[ "$(x cat m2.txt)" == "$text_2" ]] || die "m2.txt not overwritten." 
    verify_size m2.txt $text_2_len 

    # Overwrite, linux specific
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # ">" is a bash feature and exporting LD_PRELOAD in xetfs_on doesn't
        # affect the current bash process
        bash -c "echo -n $text_2 > l2.txt"
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
    echo -n $text_2 | x append t3.txt
    [[ $(cat t3.txt) == "${text_1}${text_2}" ]] || die "append to t3.txt failed"

    # Append, mmap
    echo -n $text_2 | x append-mmap m3.txt
    [[ $(cat m3.txt) == "${text_1}${text_2}" ]] || die "append to t3.txt with mmap failed" 

    # Append, linux specific. 
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # ">>" is a bash feature and exporting LD_PRELOAD in xetfs_on doesn't
        # affect the current bash process
        bash -c "echo -n $text_2 >> l3.txt"
        [[ $(cat l3.txt) == "${text_1}${text_2}" ]] || die "append to l3.txt with >> failed" 
    fi
)

# Write at specific location
assert_is_pointer_file t4.txt
assert_is_pointer_file m4.txt
assert_is_pointer_file l4.txt

(
    xetfs_on
    echo -n $text_2 | x writeat 10 t4.txt
    [[ $(cat t4.txt) == "${text_ins_at_10}" ]] || die "write at to t4.txt failed" 

    # With MMap
    echo -n $text_2 | x writeat-mmap 10 m4.txt
    [[ $(cat m4.txt) == "${text_ins_at_10}" ]] || die "write at to t4.txt failed" 

    # With existing tool dd, linux specific.
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        echo -n $text_2 | dd of=l4.txt bs=1 seek=10 conv=notrunc
        [[ $(cat l4.txt) == "${text_ins_at_10}" ]] || die "write at to l4.txt failed"
    fi
)

popd

# Write to all the files at once. 
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
    echo -n $text_2 | x write t?.txt

    for n in 1 2 3 4 ; do 
        [[ $(cat t$n.txt) == "${text_2}" ]] || die "Bulk write failed."
    done

    # MMap write to all the files at once
    echo -n $text_2 | x write-mmap m*.txt

    for n in 1 2 3 4 ; do 
        [[ $(cat m$n.txt) == "${text_2}" ]] || die "Bulk write failed."
    done
)

popd

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Write to all the files at once. 
    git xet clone --lazy $remote repo_4b
    pushd repo_4b

    for n in 1 2 3 4 ; do 
        assert_is_pointer_file t$n.txt
        assert_is_pointer_file m$n.txt
        assert_is_pointer_file l$n.txt
    done

    (
        xetfs_on
        
        # Regular write to all the files
        bash -c "cat t?.txt ; echo -n $text_2 | x write t?.txt"

        for n in 1 2 3 4 ; do 
            [[ $(cat t$n.txt) == "${text_2}" ]] || die "Bulk write failed."
        done

        # MMap write to all the files at once
        bash -c "cat t?.txt ; echo -n $text_2 | x write-mmap m*.txt"

        for n in 1 2 3 4 ; do 
            [[ $(cat m$n.txt) == "${text_2}" ]] || die "Bulk write failed."
        done
    )

    popd
fi


# Test all the sizes 
git xet clone --lazy $remote repo_5
pushd repo_5

for n in 1 2 3 4 ; do 
    assert_is_pointer_file t$n.txt
    assert_is_pointer_file m$n.txt
    assert_is_pointer_file l$n.txt
done

(
    xetfs_on

    # Regular write to all the files
    echo -n $text_2 | x write t?.txt

    for n in 1 2 3 4 ; do 
        [[ $(cat t$n.txt) == "${text_2}" ]] || die "Bulk write failed."
    done

    # MMap write to all the files at once
    echo -n $text_2 | x write-mmap m*.txt

    for n in 1 2 3 4 ; do 
        [[ $(cat m$n.txt) == "${text_2}" ]] || die "Bulk write failed."
    done
)
