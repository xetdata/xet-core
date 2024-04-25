# Development Environment
You need a rust build environment. Currently tested with Rust 1.68.2

To set up build env, switch to this directory and run:

```
python -m venv .env
source .env/bin/activate
pip install maturin
pip install fsspec
pip install ipython (for convenience)
```

To develop:

```
source ./develop.sh
```

To build locally

```
maturin develop
```

Then running python and import pyxet should work

See https://www.maturin.rs/develop.html for details

# Building a release

On mac:

```
MACOSX_DEPLOYMENT_TARGET=10.9 maturin build --release --target universal2-apple-darwin
```
