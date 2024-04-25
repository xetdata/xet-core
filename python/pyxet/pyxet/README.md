Essential implementation
------------------------

```
import pyxet

# Returns an implementation of fsspec.AbstractFileSystem.
fs = pyxet.XetFS("username/repo", branch="main", login=..., ...)

# Opens a path and file in the particular repositiory 
with fs.open("path/data.csv", 'r') as f:
    # ...

# List out the files needed.
files = fs.ls("subdir/*.json")

# Write is supported via a commit.  This uses the fsspec transaction interface. 

with fs.commit("Commit Message"):
    # Uses the fsspec transaction mechanic. 
    # Write is only enabled within such a transaction mechanic.
    with fs.open("path/new_data.csv", "wb") as f:
        f.write("...")


# We can register our interface so that outside pyxet, pandas can use the xet protocol.  
import pandas as pd

# Public repo: 
pd.read_csv("xet:://user/repo/file...")

# Private repo:
pd.read_csv("xet:://user/<repo>/file...", xet={"access_token" : "..."})

```

High level tasks
----------------

- Implement login / auth / connection (Ajit)
- Fill out methods in file_system.py. This implements
  AbstractFileSystem (https://github.com/fsspec/filesystem_spec/blob/b595ff8caa8cc7b403c9ef1b93d8aaec563187c6/fsspec/spec.py#L92)
- Implement CommitTransaction to allow writing. 
