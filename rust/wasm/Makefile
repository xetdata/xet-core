MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT_DIR := $(patsubst %/,%,$(dir $(MAKEFILE_PATH)))
LIB := lib

MOONLANDING_DIR := $(abspath ${ROOT_DIR}/../../../moon-landing)
WASM_CORE_DIR := $(abspath ${MOONLANDING_DIR}/front/xet-wasm-core)

.PHONY: local
local:
	wasm-pack build --target web

.PHONY: xetpoc
xetpoc:
	wasm-pack build --target web --out-dir ${WASM_CORE_DIR}
	cp -r ${ROOT_DIR}/${LIB} ${WASM_CORE_DIR} 
