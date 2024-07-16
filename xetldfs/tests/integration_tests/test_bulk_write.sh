#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]:-$0}")" &>/dev/null && pwd 2>/dev/null)"
. "$SCRIPT_DIR/initialize.sh"

# This gives us $remote, $text_1, $text_1_len, $text_2, $text_2_len, $all_file_text, etc. 
# It also sets XET_CAS_SIZETHRESHOLD such that $text_1 is stored in CAS and as a pointer file.
#
# Repo from in $remote has 12 files, t1.txt, t2.txt, t3.txt, t4.txt, and same for l and m instead of t.
# All are stored as pointer files and resolve in content to $text_1.
. "$SCRIPT_DIR/setup_test_repo.sh"

# Write to all the files at once. 
git xet clone --lazy $remote repo
pushd repo

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
    git xet clone --lazy $remote repo_b
    pushd repo_b

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
