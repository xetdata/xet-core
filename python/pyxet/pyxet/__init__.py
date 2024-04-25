from .file_system import open, login, XetFS, XetFSOpenFlags
from .version import __version__
from .cli import PyxetCLI, BranchCLI, RepoCLI
from .commit_transaction import MultiCommitTransaction

"""
PyXet
-----

pyxet is a Python package that provides an fsspec backend for interacting with repositories on 
XetHub. It allows users to work with files and directories on XetHub repositories using familiar 
file system operations such as open, ls, and glob. This package makes it easy to access and read 
data stored in XetHub repositories, both public and private, through a simple and intuitive API.

Main features:

1. pyxet.open: A function to open a file from a XetHub repository.
2. pyxet.XetFS: A class implementing fsspec.spec.AbstractFileSystem, 
   providing a file system handle for a XetHub repository.

To open a file from a XetHub repository, you can use the `pyxet.open()` 
function, which takes a file URL in the format 
`xet://<repo_user>/<repo_name>/<branch>/<path-to-file>`.  (See below for the URL format).

Example usage of `pyxet.open`:

```
import pyxet

# Open a file from a public repository.

f = pyxet.open('XetHub/Flickr30k/main/results.csv')

# Read the contents of the file.
contents = f.read()
f.close()
```

To work with a XetHub repository as a file system, you can use the `pyxet.XetFS` class. This class provides a file 
system handle for a XetHub repository, allowing you to perform read-only operations like ls, glob, and open. The 
initialization of this class requires a repository URL and optional arguments for branch, user, and token.

Example usage of `pyxet.XetFS`:

```
import pyxet

# Create a file system handle for a public repository.
fs = pyxet.XetFS()

# List files in the repository.
files = fs.ls('XetHub/Flickr30k/main')

# Open a file from the repository.
f = fs.open('XetHub/Flick30k/main/results.csv')

# Read the contents of the file.
contents = f.read()
```

For more information on the API and additional examples, refer to the documentation 
for `pyxet.open()` and `pyxet.XetFS.__init__()`, and the fsspec documentation. 

Fsspec Integration: 
-------------------

Many packages such as pandas and pyarrow support the fsspec protocol.  This means that 
xet:// urls can be used as file paths in these packages to refer directly to files in 
a XetHub repository.  For example, to read a csv from pandas, use: 

```
import pyxet
csv = pd.read_csv('xet://XetHub/Flickr30k/main/results.csv')
```

URLs:
-----

Xet URLs should be of the form `xet://<repo_user>/<repo_name>/<branch>/<path-to-file>`,
with the <path-to-file> being optional when opening a repository.  
The xet:// prefix is inferred as needed or if the url is given as https://.  
If branch is given as an explicit argument, it may be committed 
from the url.  

For private repos use pyxet.login to set authentication:
```
pyxet.login(user, token)
```
"""
