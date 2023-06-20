#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"

git xet install

mkdir repo
cd repo
git init
git xet init --force

function test_repo_is_configured () { 
  # Okay, make sure things are installed correctly. 
  [[ -e .git/hooks/reference-transaction ]] || die "reference-transaction hook does not exist."
  [[ -e .git/hooks/pre-push ]] || die "pre-push hook does not exist."
  [[ -e .gitattributes ]] || die ".gitattributes does not exist."
  [[ -e .git/xet ]] || die ".git/xet does not exist."

  [[ ! -z $(grep 'git-xet hook' .git/hooks/reference-transaction) ]] || die "reference-transaction hook does not have correct content."
  [[ ! -z $(grep 'git-xet hook' .git/hooks/pre-push) ]] || die "pre-push hook does not have correct content."
  [[ ! -z $(grep 'filter=xet' .gitattributes) ]] || die ".gitattributes does not have correct content."
}

test_repo_is_configured

out=$(git xet uninit 2>&1)

[[ $out == *"Successfully"* ]] || die "git xet uninstall failed."

# Test to make sure the hooks have been removed.
[[ ! -e .git/hooks/reference-transaction ]] || die "reference-transaction hook not removed."
[[ ! -e .git/hooks/pre-push ]] || die "pre-push hook not removed."
[[ -e .gitattributes ]] || die ".gitattributes should have been removed."

[[ -z $(git config --local --get-regex '.*xet.*') ]] || die "git config not purged."

# Re-init the repo
git xet init --force

test_repo_is_configured

# Now do the full uninstall
out=$(git xet uninit --full 2>&1)

[[ $out == *"Successfully"* ]] || die "git xet uninstall failed."

# Test to make sure the hooks have been removed.
[[ ! -e .git/hooks/reference-transaction ]] || die "reference-transaction hook not removed."
[[ ! -e .git/hooks/pre-push ]] || die "pre-push hook not removed."
[[ ! -e .gitattributes ]] || die ".gitattributes not removed."
[[ -z $(git config --local --get-regex '.*xet.*') ]] || die "git config not purged."

# Re-init the repo
git xet init --force

test_repo_is_configured
# Add things to the hooks; make sure those are not removed.
echo "echo \"hook run\"" >> .git/hooks/reference-transaction
echo "echo \"hook run\"" >> .git/hooks/pre-push
echo "*nothing filter=" >> .gitattributes
git commit -a -m "Modified gitattributes."

out=$(git xet uninit --full 2>&1)
[[ $out == *"Successfully"* ]] || die "git xet uninstall failed."

[[ -e .git/hooks/reference-transaction ]] || die "reference-transaction improperly removed."
[[ -e .git/hooks/pre-push ]] || die "pre-push hook improperly removed."
[[ -e .gitattributes ]] || die ".gitattributes improperly removed."

[[ ! -z $(grep "echo \"hook run\"" .git/hooks/reference-transaction) ]] || die "reference-transaction hook does not have correct content preserved."
[[ ! -z $(grep "echo \"hook run\"" .git/hooks/pre-push) ]] || die "pre-push hook does not have correct content preserved."
[[ ! -z $(grep "*nothing filter=" .gitattributes) ]] || die ".gitattributes does not have correct content preserved."









