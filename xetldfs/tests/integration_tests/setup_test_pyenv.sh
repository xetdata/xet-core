#!/usr/bin/env bash


python3 -m venv .venv
. .venv/bin/activate
pip install -q pandas numpy

setup_xetldfs_testing_env

git xet install

# file larger than 16 bytes will be checked-in as pointer file
export XET_CAS_SIZETHRESHOLD=16

# csv data
header="id,house size,house price,location,number of bedrooms"
r0="1,100,220000,Suburbs,3"
r1="2,80,180000,Suburbs,2"
r2="3,120,320000,Suburbs,4"
r3="4,65,160000,Countryside,2"

r2_new_location="98103"
r2_new="3,120,320000,98103,4"

echo -e "$header\n$r0\n$r1\n$r2\n$r3" > csv_data.csv
csv_data_file_size=$(file_size csv_data.csv)

# matrix data
m0="24.73 52.92 79.57 50.68 76.68"
m1="98.43 69.44 42.23 43.54 54.18"
m2="54.86 21.40 43.87 69.93 44.01"
m3="80.23 32.28 77.86 11.32 26.75"
m4="58.36 60.91 66.72 71.58 45.57"
echo -e "$m0\n$m1\n$m2\n$m3\n$m4" > data.mat
mat_data_file_size=$(file_size data.mat)
mat_shape="5 5"
flat_mat="$m0 $m1 $m2 $m3 $m4"

remote=$(create_bare_repo)

git clone $remote repo_setup

pushd repo_setup
[[ $(git branch) == *"main"* ]] || git checkout -b main
git xet init --force
git push origin main # This created a commit, so push it to main.

for n in 1 2; do
    cp ../csv_data.csv f$n.csv
    cp ../data.mat f$n.mat
done

git add .
git commit -m "add csv data"
git push origin main
popd

# Some helper functions

py_verify_size() {
    file=$1
    expected_len=$2
    (
        xetfs_on
        len=$(python3 -c "import os; print(os.stat('$file').st_size)")
        [[ $len == $expected_len ]] || die "os.stat length of $file is wrong; got $len, expected $expected_len"

        len=$(python3 -c "import os; print(os.path.getsize('$file'))")
        [[ $len == $expected_len ]] || die "os.path.getsize length of $file is wrong; got $len, expected $expected_len"

        #len=$(python3 -c "import os;fp=open('$file');fp.seek(0, os.SEEK_END);print(fp.tell())")
        #[[ $len == $expected_len ]] || die "open-seek-tell length of $file is wrong; got $len, expected $expected_len"

        len=$(python3 -c "from pathlib import Path;print(Path('$file').stat().st_size)")
        [[ $len == $expected_len ]] || die "pathlib.Path.stat length of $file is wrong; got $len, expected $expected_len"
    )
}

py_run() {
    python3 -c "$1"
}