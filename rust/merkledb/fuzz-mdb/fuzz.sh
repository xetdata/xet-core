#!/usr/bin/env bash
export AFL_TMPDIR=/tmp/fuzz_mdb/memtmp
export AFL_TESTCACHE_SIZE=1000
export AFL_AUTORESUME=1
export AFL_INPUT_LEN_MIN=2 # 2 bytes reserved for repetition count - minimum 0 for random data
export AFL_INPUT_LEN_MAX=50000 # ~50kb max
cargo afl build && cargo afl fuzz -i in -o out -t 2000 ../../target/debug/fuzz-mdb
