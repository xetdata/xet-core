MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT_DIR := $(patsubst %/,%,$(dir $(MAKEFILE_PATH)))
LIB := lib

MOONLANDING_DIR := $(abspath ${ROOT_DIR}/../../../moon-landing)

.PHONY: local
local:
	wasm-pack build --target web

foo:
	echo ${ROOT_DIR}
	echo ${ROOT_DIR}/${LIB}
	echo ${MOONLANDING_DIR}

.PHONY: xetpoc
xetpoc:
	wasm-pack build --target web --out-dir ${MOONLANDING_DIR}front/xet-wasm-core
	cp -r ${ROOT_DIR}${LIB} ${MOONLANDING_DIR}front/xet-wasm-core/
