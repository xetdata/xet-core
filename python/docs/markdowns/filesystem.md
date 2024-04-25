# File system

Pyxet implements a simple API based on the [fsspec](https://filesystem-spec.readthedocs.io/en/latest/)
library. Use it to access local files, remote files, and files in XetHub.

## Using URLs

Xet URLs are in the form:
```sh
xet://<repo_owner>/<repo_name>/<branch>/<path_to_file>
```

The `<path_to_file>` argument is optional if the URL
refers to a repository and the `xet://` prefix is optional when using pyxet.XetFS.

## Accessing private repositories

To create your own repositories or access private repositories, first [create a XetHub account and set your personal access token](quickstart.md).

## pyxet.XetFS

To work with a XetHub repository as a file system, you can use the `pyxet.XetFS` class. This class provides a file
system handle
for a XetHub repository, allowing you to perform opens, reads, and writes. The initialization of
this class
requires a repository URL and optional arguments for branch, user, and token. All write operations will 
automatically commit the change back to XetHub; the optional commit message will be applied when available.

Example usage of `pyxet.XetFS`:

```python
  import pyxet

  # Create a file system handle for a repository
  fs = pyxet.XetFS()

  # List files in the repository.
  files = fs.ls('xet://XetHub/Flickr30k/main')

  # Open a file from the repository.
  f = fs.open('xet://XetHub/Flickr30k/main/results.csv')

  # Read the contents of the file.
  contents = f.read()

  # Write to a repository with an optional commit message
with fs.transaction as tr:
    tr.set_commit_message("Writing things")
    fs.open("<user_name>/<repo_name>/main/foo", 'w').write("Hello world!")
```

## Other common utilities
```python
  import pyxet

  fs = pyxet.XetFS()  # fsspec filesystem

  # Read functions
  fs.info("xethub/titanic/main/titanic.csv")
  # returns repo level info: {'name': 'https://xethub.com/XetHub/titanic/titanic.csv', 'size': 61194, 'type': 'file'}

  fs.open("XetHub/titanic/main/titanic.csv", 'r').read(20)
  # returns first 20 characters: 'PassengerId,Survived'

  fs.get("XetHub/titanic/main/data/", "data", recursive=True)
  # download remote directory recursively into a local data folder

  fs.ls("XetHub/titanic/main/data/", detail=False)
  # returns ['data/titanic_0.parquet', 'data/titanic_1.parquet']

  # Write functions, with optional commit message
  with fs.transaction as tr:
    tr.set_commit_message("Write hi")
    fs.open("<user_name>/<repo_name>/main/text.txt", 'w').write("Hello world!")
  # writes "Hello World" to text.txt, Git commits the change with comment "Write hi" in the main branch of the repository

  with fs.transaction as tr:
    tr.set_commit_message("Copy file")
    fs.cp("<user_name>/<repo_name>/main/text.txt", "<user_name>/<repo_name>/main/text2.txt")
  # copies text.txt into text2.txt in the main branch of the repository, commits the change with "Copy file"

  with fs.transaction as tr:
    tr.set_commit_message("Remove file")
    fs.rm("<user_name>/<repo_name>/main/titanic2.csv")
  fs.info("XetHub/titanic/main/titanic2.csv") 
   # removes a file from the main branch of the repository with comment "Remove file"
```

## [fsspec](https://filesystem-spec.readthedocs.io/en/latest/usage.html)

Many packages such as pandas and pyarrow support the fsspec protocol.
xet:// URLs must be used as file paths when interacting with these packages. For example, to read a CSV from pandas, use:

```sh
  import pyxet   # make xet protocol available to fsspec
  import pandas as pd

  df = pd.read_csv('xet://XetHub/Flickr30k/main/results.csv')
```

