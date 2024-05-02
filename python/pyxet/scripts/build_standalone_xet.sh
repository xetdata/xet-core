#!/bin/bash -ex

if [[ ! -e pyproject.toml ]] ; then 
    echo "Run this script in the pyxet directory using ./scripts/$0"
    exit 1
fi

# Clear out the old virtual env.
rm -rf .venv_pyinstaller

OS=$(uname -s)

if [[ -z "$PYTHON_EXECUTABLE" ]] ; then 
    if [[ "$OS" == "Darwin" ]]; then
        # Use system universal one
        PYTHON_EXECUTABLE=/usr/bin/python3
    else 
        PYTHON_EXECUTABLE=$(which python3)
    fi
fi

$PYTHON_EXECUTABLE -m venv .venv_pyinstaller

. .venv_pyinstaller/bin/activate

pip install --upgrade pip
pip install maturin==0.14.17 fsspec pyinstaller pytest cloudpickle s3fs tabulate typer

# Clear out any old wheels
mv target/wheels/ target/old_wheels/ || echo ""

if [[ "$OS" == "Darwin" ]]; then
    maturin build --release --target=universal2-apple-darwin --features=openssl_vendored
else 
    maturin build --release --features=openssl_vendored
fi

pip install target/wheels/pyxet-*.whl

# Run tests.
# pytest tests/

# Build binary
if [[ "$OS" == "Darwin" ]]; then
    pyinstaller --onefile "$(which xet)" --target-arch universal2
else 
    pyinstaller --onefile "$(which xet)" 
fi
