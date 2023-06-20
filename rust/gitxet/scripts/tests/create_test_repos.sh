#!/usr/bin/env bash
set -e
set -x

if [[ $# != 1 ]] ; then 
  echo "Usage: $0 <directory>"
fi

script_dir=$(dirname "$0")/../

mkdir -p $1
pushd $1
base_dir=$PWD


mkdir -p $base_dir/origin
cd $base_dir/origin
git init --bare
cd $base_dir

mkdir -p $base_dir/cas
cas="local://$base_dir/cas/"


for i in 1 2 ; do 
  repo=repo_$i
  git clone origin $repo

  pushd $repo

  git-xet init --cas=$cas

  popd
done



