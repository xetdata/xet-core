#!/usr/bin/env bash
set -e
set -x

if [[ $# != 1 ]] ; then 
  >&2 echo "Usage:"
  >&2 echo "      $0 local              Run all tests using a local repository."
  >&2 echo "      $0 <xetsvc remote>    Run all tests using the remote repository.  Note that all contents will be erased."
  exit 1
fi


SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";

scripts=$(cd $SCRIPT_DIR && ls test_*sh)

>&2 echo "Running these scripts: "
for s in $scripts ; do 
  >&2 echo "    $s"
done

remote=$1
if [[ $remote != local ]] ; then 
  >&2 echo "Using remote $remote."

  if [[ ! $remote = *"xetsvc"* ]] ; then 
    >&2 echo "WARNING: Remote does not have xetsvc as endpoint."
  fi
  
  export XET_TESTING_REMOTE=$remote
else
  >&2 echo "Using local repository for remotes."
fi

failed_tests=()
passed_tests=()

for test in $scripts ; do 
  dir=${test%%.}
  mkdir -p $dir/
  pushd $dir

  2>&1 echo "#############################################################################"
  2>&1 echo "##                                                                         "
  2>&1 echo "## Running $test                                               "
  2>&1 echo "##                                                                         "
  2>&1 echo "#############################################################################"


  bash -e -x $SCRIPT_DIR/$test 

  if [[ $? != 0 ]] ; then
    2>&1 echo "#############################################################################"
    2>&1 echo "## $test: FAILED                                              "
    2>&1 echo "#############################################################################"
    failed_tests+=($test)
  else 
    2>&1 echo "#############################################################################"
    2>&1 echo "## $test: PASSED                                              "
    2>&1 echo "#############################################################################"
    passed_tests+=($test)
  fi
  popd 
done

any_failed=0

for test in ${passed_tests[@]} ; do 
  >&2 echo "PASSED: $test"
done

for test in ${failed_tests[@]} ; do 
  >&2 echo "FAILED: $test"
  any_failed=1
done

exit $any_failed 








