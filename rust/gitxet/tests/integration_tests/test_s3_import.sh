#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"


mkdir s3_test_repo
cd s3_test_repo

git init
git xet init --force

bucket="s3://xethub-s3-testing-dataset/"

if aws s3 ls $bucket ; then 
  echo "AWS bucket accessable."
else 
  2>&1 echo "WARNING: AWS bucket not accessable or aws cli not present; skipping S3 Import test."
  exit 0
fi

# Retrieve the data through our methods
git xet s3 import $bucket xet_s3_data/data/

# Retrieve the data through AWS cli
mkdir -p aws_s3_data/data/
aws s3 sync $bucket aws_s3_data/data/

# make sure they are the same. 
xet_md5=$(cd xet_s3_data/ && md5 data/*)
aws_md5=$(cd aws_s3_data/ && md5 data/*)

if [[ ! "$xet_md5" == "$aws_md5" ]] ; then 
  die "Downloaded data appears to be different: 

$xet_md5

  versus

$aws_md5
  "
fi

