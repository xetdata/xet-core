#!/usr/bin/env bash
set -e

usage() { 
  echo "Usage:  $0 --cas=<cas info>" 
  exit 1
}

if [[ $# != 1 || $1 != --cas=* ]] ; then 
  usage
fi

cas=${1##--cas=}

require_clean_work_tree () {
    # Update the index
    git update-index -q --ignore-submodules --refresh
    err=0

    # Disallow unstaged changes in the working tree
    if ! git diff-files --quiet --ignore-submodules --
    then
        echo >&2 "Cannot $1: you have unstaged changes."
        git diff-files --name-status -r --ignore-submodules -- >&2
        err=1
    fi

    # Disallow uncommitted changes in the index
    if ! git diff-index --cached --quiet HEAD --ignore-submodules --
    then
        echo >&2 "Cannot $1: your index contains uncommitted changes."
        git diff-index --cached --name-status -r --ignore-submodules HEAD -- >&2
        err=1
    fi

    if [ $err = 1 ]
    then
        echo >&2 "Please commit or stash them."
        exit 1
    fi
}


CURRENT_GIT_DIR=$(git rev-parse --git-dir)
CURRENT_REPO_DIR=$(git rev-parse --show-toplevel)
cd $CURRENT_REPO_DIR

echo "Configuring git repository to use Xet with Content Store $cas."

# Require a clean work tree
# require_clean_work_tree "configure repository to use Xet"


# TODO: this needs to be robust.
git config --add remote.origin.fetch "+refs/notes/xet/*:refs/remotes/origin/notes/xet/*"

mkdir -p "$CURRENT_GIT_DIR/xet/"
mkdir -p "$CURRENT_GIT_DIR/xet/scripts/"
mkdir -p "$CURRENT_GIT_DIR/xet/staging/"

echo '* filter=xet -text
*.gitattributes filter= 
' >> "$CURRENT_REPO_DIR/.gitattributes"


###########################################
# Add in various plumbing scripts.

# Sync the mdb to the notes
echo "#!/usr/bin/env bash
  if [[ \$GIT_XET_TRACE == 1 ]] ; then
    set -x
  fi
  current_git_dir=\"\$(git rev-parse --git-dir)\"
 
  git-xet merkle-db update-git \"\$current_git_dir/xet/merkledb.db\" || >&2 echo \"Updating MerkleDB failed.\"
" > $CURRENT_GIT_DIR/xet/scripts/sync_mdb_to_notes.sh
chmod a+x $CURRENT_GIT_DIR/xet/scripts/sync_mdb_to_notes.sh

# Synchronize the notes.  Takes remote as parameter
echo "#!/usr/bin/env bash
  if [[ \$GIT_XET_TRACE == 1 ]] ; then
    set -x
  fi
  
  if [[ -z \$1 ]] ; then
    remote=origin
  else
    remote=\$1
  fi
  
  current_git_dir=\"\$(git rev-parse --git-dir)\"

  \"\$current_git_dir/xet/scripts/sync_remote_to_notes.sh\" \$remote

  remote_ref=\$(git show-ref -s refs/remotes/\$remote/notes/xet/merkledb)
  if [[ ! -z \$remote_ref ]] ; then 
    git notes --ref=xet/merkledb merge \$remote_ref 1>&2 || >&2 echo \"failed\" 
  fi

  remote_ref=\$(git show-ref -s refs/remotes/\$remote/notes/xet_alt/merkledb)
  if [[ ! -z \$remote_ref ]] ; then 
    git notes --ref=xet/merkledb merge \$remote_ref 1>&2 || >&2 echo \"failed\" 
  fi

  git-xet --merkledb=\"\$current_git_dir/xet/merkledb.db\" --cas=$cas push
  git push --no-verify \$remote \"refs/notes/xet/*\"
" > $CURRENT_GIT_DIR/xet/scripts/sync_notes_to_remote.sh
chmod a+x $CURRENT_GIT_DIR/xet/scripts/sync_notes_to_remote.sh

echo "#!/usr/bin/env bash
  if [[ \$GIT_XET_TRACE == 1 ]] ; then
    set -x
  fi

  if [[ -z \$1 ]] ; then
    remote=origin
  else
    remote=\$1
  fi

  remote_ref=\$(git show-ref -s refs/remotes/\$remote/notes/xet/merkledb)
  if [[ ! -z \$remote_ref ]] ; then 
    git notes --ref=xet/merkledb merge \$remote_ref 1>&2 || >&2 echo \"failed\" 
  fi
  
  remote_ref=\$(git show-ref -s refs/remotes/\$remote/notes/xet_alt/merkledb)
  if [[ ! -z \$remote_ref ]] ; then 
    git notes --ref=xet/merkledb merge \$remote_ref 1>&2 || >&2 echo \"failed\" 
  fi
  
  current_git_dir=\"\$(git rev-parse --git-dir)\"

  git-xet merkle-db extract-git \"\$current_git_dir/xet/merkledb.db\" 1>&2 || >&2 echo \"failed\" 
" > $CURRENT_GIT_DIR/xet/scripts/sync_notes_to_mdb.sh
chmod a+x $CURRENT_GIT_DIR/xet/scripts/sync_notes_to_mdb.sh

# Takes remote as parameter
echo "#!/usr/bin/env bash
  if [[ \$GIT_XET_TRACE == 1 ]] ; then
    set -x
  fi

  if [[ -z \$1 ]] ; then
    remote=origin
  else
    remote=\$1
  fi

  # --refmap=\"\" is needed to avoid triggering automatic fetch in remote.origin.fetch option.
  # fetching to xet_alt is needed to avoid conflicts with the remote.origin.fetch process fetching to notes/xet/
  git fetch \$remote --refmap= --no-write-fetch-head \"+refs/notes/xet/*:refs/remotes/\$remote/notes/xet_alt/*\" 
  
" > $CURRENT_GIT_DIR/xet/scripts/sync_remote_to_notes.sh
chmod a+x $CURRENT_GIT_DIR/xet/scripts/sync_remote_to_notes.sh

# Takes remote as parameter
echo "#!/usr/bin/env bash
  if [[ \$GIT_XET_TRACE == 1 ]] ; then
    set -x
  fi
  
  current_git_dir=\"\$(git rev-parse --git-dir)\"

  \"\$current_git_dir/xet/scripts/sync_remote_to_notes.sh\" \$1
  \"\$current_git_dir/xet/scripts/sync_notes_to_mdb.sh\" \$1
" > $CURRENT_GIT_DIR/xet/scripts/sync_remote_to_mdb.sh
chmod a+x $CURRENT_GIT_DIR/xet/scripts/sync_remote_to_mdb.sh


echo "#!/usr/bin/env bash
tr=\$(cat /dev/stdin)

current_git_dir=\"\$(git rev-parse --git-dir)\"

# echo \$tr
# we detect committed reference transactions only
if [[ \$1 == \"committed\" ]]; then
  # look for changes to refs/remotes
  if [[ \$tr =~ \"refs/remotes\" ]]; then
    
    # ignore changes to refs/remotes/.../notes/... so this does not become recursive
    if [[ ! \$tr =~ \"notes\" ]]; then

      if [[ \$GIT_XET_TRACE == 1 ]] ; then 
        \"\$current_git_dir/xet/scripts/sync_remote_to_notes.sh\" || >&2 echo \"Remote sync of notes failed.\"
      else
        \"\$current_git_dir/xet/scripts/sync_remote_to_notes.sh\" 2>&1 > /dev/null
      fi 

    fi
  fi
fi
" > $CURRENT_GIT_DIR/xet/scripts/reference_transaction_hook.sh
chmod a+x $CURRENT_GIT_DIR/xet/scripts/reference_transaction_hook.sh

echo "#!/usr/bin/env bash

current_git_dir=\"\$(git rev-parse --git-dir)\"

if [[ \$GIT_XET_TRACE == 1 ]] ; then 
   set -x
   \"\$current_git_dir/xet/scripts/sync_notes_to_mdb.sh\" || >&2 echo \"Warning: failed merging notes.\"
else
   \"\$current_git_dir/xet/scripts/sync_notes_to_mdb.sh\" 2>/dev/null 1>/dev/null
fi

mod_time=\$(stat \"\$current_git_dir/xet/merkledb.db\" 2>/dev/null || echo \"error\")

git-xet \"--merkledb=\$current_git_dir/xet/merkledb.db\" \"--cas=$cas\" filter

# If there are changes, flush it.
new_mod_time=\$(stat \"\$current_git_dir/xet/merkledb.db\" 2>/dev/null || echo \"error 2\")

if [[ \$mod_time != \$new_mod_time ]] ; then 
  if [[ \$GIT_XET_TRACE == 1 ]] ; then 
    \"\$current_git_dir/xet/scripts/sync_mdb_to_notes.sh\" || >&2 echo \"Warning: failed uploading merkledb.\"
  else
    \"\$current_git_dir/xet/scripts/sync_mdb_to_notes.sh\" 2>/dev/null 1>/dev/null
  fi
fi
" > $CURRENT_GIT_DIR/xet/scripts/filter_process.sh
chmod a+x $CURRENT_GIT_DIR/xet/scripts/filter_process.sh


#########################################
#  Install all the hooks

echo "#!/usr/bin/env bash
if [[ \$GIT_XET_TRACE == 1 ]] ; then 
  set -x
fi

current_git_dir=\"\$(git rev-parse --git-dir)\"

# This hook is triggered on push from another repository as well.  However, 
# in that case, it is not run from the local repository, but rather the
# \"\$current_git_dir/ folder of the repository pushing to us.  When that is the case, we 
# should ignore this hook.
if [[ ! -e \"\$current_git_dir/xet/\" ]] ; then
  exit 0
fi

if [[ \$GIT_XET_TRACE == 1 ]] ; then 
  \"\$current_git_dir/xet/scripts/reference_transaction_hook.sh\" \$@
else 
  \"\$current_git_dir/xet/scripts/reference_transaction_hook.sh\" \$@ 2>/dev/null 1>/dev/null
fi

" > $CURRENT_GIT_DIR/hooks/reference-transaction
chmod a+x $CURRENT_GIT_DIR/hooks/reference-transaction

echo "#!/usr/bin/env bash
if [[ \$GIT_XET_TRACE == 1 ]] ; then 
  set -x
fi
  
current_git_dir=\"\$(git rev-parse --git-dir)\"

if [[ \$GIT_XET_TRACE == 1 ]] ; then 
  \"\$current_git_dir/xet/scripts/sync_mdb_to_notes.sh\" 
  \"\$current_git_dir/xet/scripts/sync_notes_to_remote.sh\" \$@
else 
  \"\$current_git_dir/xet/scripts/sync_mdb_to_notes.sh\" 2>/dev/null 1>/dev/null
  \"\$current_git_dir/xet/scripts/sync_notes_to_remote.sh\" \$@ 2>/dev/null 1>/dev/null
fi
" > $CURRENT_GIT_DIR/hooks/pre-push
chmod a+x $CURRENT_GIT_DIR/hooks/pre-push

#########################################
# Install the filter.

echo "[filter \"xet\"]
    process = \"$CURRENT_GIT_DIR/xet/scripts/filter_process.sh\"
    required
  " >> $CURRENT_GIT_DIR/config

# Finally, refresh things.
pushd $CURRENT_GIT_DIR/..
$CURRENT_GIT_DIR/xet/scripts/sync_remote_to_notes.sh

echo "Repository configured; checking out files."
# git checkout HEAD -- "$(git rev-parse --show-toplevel)"



