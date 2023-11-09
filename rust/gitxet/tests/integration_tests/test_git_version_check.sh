#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"
setup_basic_run_environment


# Now set the path and create a fake git wrapper that incorrectly reports the version. 

GIT=`which git`
mkdir -p $PWD/bin
export XET_GIT_EXECUTABLE=$PWD/bin/git

write_versioned_git_shim () { 
  echo "#!/usr/bin/env bash

  if [[ \$1 == \"--version\" ]] ; then 
    echo \"$1\"
    exit 0
  else 
    $GIT \"\$@\"
  fi
  " > $PWD/bin/git

  chmod a+x $PWD/bin/git
}

write_versioned_git_shim "git version 2.29" 
git xet install || die "Should have succeeded with (fake) git version 2.29."

write_versioned_git_shim "git version 2.28" 
git xet install && die "Should have failed with (fake) git version 2.28." || echo "Okay."

write_versioned_git_shim "git version 2.29.1" 
git xet install || die "Should have succeeded with (fake) git version 2.29.1."

write_versioned_git_shim "git version 3.0.1alpha1" 
git xet install || die "Should have succeeded with (fake) git version 3.0.1alpha1"

write_versioned_git_shim "git version 2.30dev4" 
git xet install || die "Should have succeeded with (fake) git version 2.30dev4" 

write_versioned_git_shim "git version 0.2.29beta1" 
git xet install && die "Should have failed with (fake) git version 0.2.29beta1" || echo "Okay."


export XET_NO_GIT_VERSION_CHECK=1

write_versioned_git_shim "git version 2.28" 
git xet install || die "Should have succeeded with no version check."

write_versioned_git_shim "git version 0.2.29beta1" 
git xet install || die "Should have succeeded with no version check."
