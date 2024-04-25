Pyxet is a Python library that provides a pythonic interface for
[XetHub](https://xethub.com/).  Xethub is simple git-based system capable of
storing TBs of ML data and models in a single repository, with block-level
data deduplication that enables hundreds of versions of similar data to be
stored without requiring much storage.

The pyxet library has 3 components:

1. A [fsspec](https://filesystem-spec.readthedocs.io)
interface that allows compatible libraries such as Pandas, Polars and Duckdb
to directly access any version of any file in a Xet repository. See below
for some examples.

2. A command line interface inspired by AWSCLI that allows files to be
uploaded to and downloaded from Xet repository conveniently and efficiently.

3. A file system mount mechanism that allows any version of any Xet repository
to be mounted. This works on Mac, Linux, and Windows 11 Pro.

For API documentation and full examples, please see [here](https://pyxet.readthedocs.io/en/latest/).
