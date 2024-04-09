#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"
setup_basic_run_environment

# Test that it succeeds only with force on a regular remote.
mkdir origin
pushd origin
git init --bare
popd 

git clone origin repo_1 

pushd repo_1
git xet init  && die "Failed to prohibit non-xet remote." || echo "Failed correctly."
[[ ! -e .gitattributes ]] || die ".gitattributes file created improperly."
git xet init --force || die "Init should succeed with --force." 
[[ -e .gitattributes ]] || die ".gitattributes file not created."
popd

# Test that it succeeds only with force on a bare remote.
mkdir no_remotes
pushd no_remotes
git init
git xet init  && die "Failed to prohibit non-xet remote." || echo "Failed correctly."
[[ ! -e .gitattributes ]] || die ".gitattributes file created improperly."
git xet init --force || die "Init should succeed with --force." 
[[ -e .gitattributes ]] || die ".gitattributes file not created."
popd

# Test that it succeeds on each of the services.
for remote in xethub.com xetsvc.com xetbeta.com ; do 

  rm -rf repo_test test_dir
  mkdir test_dir
  pushd test_dir
  mkdir $remote
  cd $remote
  git init --bare
  popd

  git clone test_dir/$remote repo_test

  pushd repo_test
  git xet init || die "Init should succeed with remote $remote" 
  [[ -e .gitattributes ]] || die ".gitattributes file not created."
  popd
done

