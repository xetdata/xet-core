#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"

# Install this into the new global config .gitconfig
if git lfs install ; then 
  echo "installed git lfs"
else 
  echo "Failure installing lfs; exiting quietly."
  exit 0
fi  

# Fail silently if reaching github does not work
if git clone git@github.com:xetdata/xet_lfs_import_test.git ; then
  echo "Repository successfully cloned."
else 
  echo "Failure cloning repository from github; exiting quietly."
  exit 0
fi

cd xet_lfs_import_test


git xet init --force

# Now, ensure that we actually test for things on smudge

rm random.data
test_out=$(git reset --hard 2>&1)

if [[ ! $test_out = *"lfs"* ]] ; then 
   die "Output does not contain LFS instructions: \n($test_out)"
fi

git lfs fetch 
git lfs checkout 

hash=$(cat random.data | md5)
ref_hash=$(cat random_data.hash)

if [[ ! $hash == $ref_hash ]] ; then 
  die "git lfs checkout failed"
fi

git commit -a -m "Update."



