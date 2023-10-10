#!/usr/bin/env bash
export AFL_TMPDIR=/tmp/fuzz_mdb/memtmp
export AFL_TESTCACHE_SIZE=1000
export AFL_AUTORESUME=1
cargo afl build && cargo afl fuzz -i in -o out -t 2000 ../../target/debug/fuzz-mdb
