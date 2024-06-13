#!/bin/bash -e 

# Function to check if the version string is valid
is_valid_version() {
    local version=$1
    if [[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+ ]]; then
        return 0
    else
        return 1
    fi
}

print_help() {
    echo "Usage: $0 <new version>"
    echo "If this was a major release, update increment the second number, Eg: 0.7.9->0.8.0"
    echo "If this was a minor release, update increment the third number, Eg: 0.7.9->0.7.10"
}


export XET_NEXT_VERSION=$1

if [[ ! -d ./rust/gitxetcore/ ]] ; then 
  echo "Run this script in the base of the repo as ./scripts/$0"
  exit 1
fi

if [[ "$1" == "--help" ]] ; then
  print_help
  exit 1
fi

base_dir="$PWD"

cargo install cargo-get

current_version=$(cd gitxet && cargo get package.version)

# Check if a parameter is provided
if [[ -z "$1" ]] ; then
   print_help
   echo "The current version of the repository is $current_version."
   exit 1
fi

# Validate the version string
if ! is_valid_version "$1"; then
    echo "Error: Invalid version string." 
    echo "Usage: $0 <new version>"
    exit 1
fi

echo "Current version is $current_version, tagging and upgrading repository to $XET_NEXT_VERSION."

# tag the current version and release the next one.
git tag v$current_version && git push --tags --force || echo "Error creating previous tag."

# increment the version from before
echo "Updated xet-core and dependencies to version $XET_NEXT_VERSION"

git checkout main
git pull

# ensure main is clean
git status --porcelain

git checkout -b update-version-$XET_NEXT_VERSION

# Update project Cargo.toml files and Cargo.lock file
# Replace only the `version = "..."` string in the first 5 lines to avoid 
# replacing the version line for a dependency.
find . -name 'Cargo.toml' | xargs sed -i '' "1,5s/^version = \"${current_version}\"$/version = \"$XET_NEXT_VERSION\"/" 

pushd rust && cargo build && popd
pushd libxet && cargo build && popd
pushd gitxet && cargo build && popd
 
git commit -a -m "Post release: updating version to $XET_NEXT_VERSION"

echo "Repository updated to version $XET_NEXT_VERSION.  Please check by running:"
echo
echo " git diff origin/main "
echo 
echo "Then create a PR with git push."

