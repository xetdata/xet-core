#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
. "$SCRIPT_DIR/initialize.sh"

remote=$(create_bare_repo)

if [[ -z $(which python3) ]] ; then
   echo "test_dir_summaries.sh requires python, skipping."
   exit 0
fi

git clone $remote repo_1


cd repo_1

# Create stuff for the dir summary
mkdir foo
mkdir foo/bar
mkdir foo/rebar
mkdir morefoo
mkdir morefoo/bar


echo "Some Text" > foo/text_1.txt
echo "More Text" > foo/bar/text_2.txt
echo "Even more Text" > foo/bar/text_3.txt
echo "Yet more text" > foo/rebar/text_4.txt
echo "No way it's more text" > morefoo/text_5.txt
echo "No way it's still more text" > morefoo/bar/text_6.txt

git add *
git commit -a -m "Added data."

function get_key {
  cat dir_summary.json | python3 -c "import sys, json; print(json.load(sys.stdin)['$1']['FileTypeSimple']['Text File'])"
}

# Test non-recursive
git xet dir-summary > dir_summary.json

[[ $(get_key foo/bar) == "2" ]] || die 'bad key foo/rebar'
[[ $(get_key foo/rebar) == "1" ]] || die 'bad key foo/rebar'
[[ $(get_key foo) == "1" ]] || die 'bad key foo'
[[ $(get_key morefoo/bar) == "1" ]] || die 'bad key morefoo/bar'
[[ $(get_key morefoo) == "1" ]] || die 'bad key morefoo/bar'

# Test recursive
git xet dir-summary --recursive > dir_summary.json

[[ $(get_key foo/bar) == "2" ]] || die 'bad key foo/rebar'
[[ $(get_key foo/rebar) == "1" ]] || die 'bad key foo/rebar'
[[ $(get_key foo) == "4" ]] || die 'bad key foo'
[[ $(get_key morefoo/bar) == "1" ]] || die 'bad key morefoo/bar'
[[ $(get_key morefoo) == "2" ]] || die 'bad key morefoo/bar'

echo "Some Text" > foo/text_1b.txt
echo "More Text" > foo/bar/text_2b.txt
echo "Even more Text" > foo/bar/text_3b.txt
echo "Yet more text" > foo/rebar/text_4b.txt

git add *
git commit -a -m "Added more data."

# Test non-recursive
git xet dir-summary > dir_summary.json

[[ $(get_key foo/bar) == "4" ]] || die 'bad key foo/rebar'
[[ $(get_key foo/rebar) == "2" ]] || die 'bad key foo/rebar'
[[ $(get_key foo) == "2" ]] || die 'bad key foo'
[[ $(get_key morefoo/bar) == "1" ]] || die 'bad key morefoo/bar'
[[ $(get_key morefoo) == "1" ]] || die 'bad key morefoo/bar'

# Test recursive
git xet dir-summary --recursive > dir_summary.json

[[ $(get_key foo/bar) == "4" ]] || die 'bad key foo/bar'
[[ $(get_key foo/rebar) == "2" ]] || die 'bad key foo/rebar'
[[ $(get_key foo) == "8" ]] || die 'bad key foo'
[[ $(get_key morefoo/bar) == "1" ]] || die 'bad key morefoo/bar'
[[ $(get_key morefoo) == "2" ]] || die 'bad key morefoo/bar'
