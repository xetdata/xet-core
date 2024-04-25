# CLI

The pyxet CLI is a command line interface for interacting with XetHub repositories like a blob stores, while leveraging the
power of Git branches and versioning.   
The CLI follows [fsspec](https://filesystem-spec.readthedocs.io/en/latest/) standards.

```bash
$ xet --help
Usage: xet [OPTIONS] COMMAND [ARGS]...

╭─ Commands ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ branch         sub-commands to manage branches                                                                                │
│ cat            Prints a file to stdout                                                                                        │
│ cp             copy files and folders                                                                                         │
│ info           Provide information about a project branch                                                                     │
│ login          Configures the login information. Stores the config in ~/.xetconfig                                            │
│ ls             list files and folders                                                                                         │
│ mount          Mounts a repository on a local path                                                                            │
│ mv             move files and folders                                                                                         │
│ repo           sub-commands to manage repositories                                                                            │
│ rm             delete files and folders                                                                                       │
│ version        Prints the current Xet-cli version                                                                             │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

## ls (list)

*ls* can list local and remote files and even list branches and repositories.

```bash
list files and folders
╭─ Arguments ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│   path      [PATH]  Source file or folder which will be copied [default: xet://]                                              │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --raw     --no-raw      If True, will print the raw JSON output [default: no-raw]                                             │
│ --help                  Show this message and exit.                                                                           │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

### Usage

```bash

# list files and directories
$ xet ls xet://user/repo/main/path/to/file/or/dir
# list repos
$ xet ls xet://user/

# list organisation users
$ xet ls 
# list all available repos for current user + organisation
$ xet repo ls
# list all available branches for a project
$ xet branch ls xet://user/repo

# examples
$ xet ls xet://xdssio/gitease/giteas
xdssio/gitease/gitease/__init__.py         2  file
xdssio/gitease/gitease/cli.py           6146  file

$ xet ls xet://xdssio
name                          type
----------------------------  ------
xdssio/datasets               repo
xdssio/FastChat               repo
...
```

Note that this works with S3 buckets, or anything fsspec understands as well!

```
$ xet ls s3://<bucket>
```

## cp (copy)

*cp* copy local and remote files and even copy between branches or repositories (if they exists).

* For directories use the `-r` flag.

```
 copy files and folders

╭─ Arguments ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *    source      TEXT  Source file or folder which will be copied [default: None] [required]                                  │
│ *    target      TEXT  Target location of the file or folder [default: None] [required]                                       │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --recursive  -r               Recursively copy files and folders                                                              │
│ --message    -m      TEXT     A commit message                                                                                │
│ --parallel   -p      INTEGER  Maximum amount of parallelism [default: 32]                                                     │
│ --help                        Show this message and exit.                                                                     │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

### Usage

```bash
# Copy files or directories
$ xet cp xet://user/repo/branch/path/to/source xet://user/repo/branch/path/to/target

# examples
$ xet cp xet://xdssio/titanic/experiment-1/titanic.csv xet://xdssio/titanic/experiment-2/titanic.csv
Copying xdssio/titanic/experiment-1/titanic.csv to xdssio/titanic/experiment-2/titanic.csv...
```

Since fsspec understand S3, `cp` can be used to upload and download files from S3 buckets too:
```
# upload to S3
$ xet cp xet://... s3://...
# download from S3
$ xet cp s3://... xet://...
```

## mv (move)

*mv* move remote files **within the same branch** in a repository.

```bash
 move files and folders

╭─ Arguments ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *    source      TEXT  Source Xet file or folder to move [default: None] [required]                                           │
│ *    target      TEXT  Target location or name to move to [default: None] [required]                                          │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --recursive  -r            Recursively copy files and folders                                                                 │
│ --message    -m      TEXT  A commit message                                                                                   │
│ --help                     Show this message and exit.                                                                        │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

### Usage

```bash

$ xet mv xet://user/repo/branch/path/to/source xet://user/repo/branch/path/to/target

# examples 
$ xet mv xet://xdssio/titanic/experiment-1/titanic.csv xet://xdssio/titanic/experiment-1/titanic2.csv
```

## rm (delete)

```bash

 delete files and folders

╭─ Arguments ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *    paths      PATHS...  File or folder which will be deleted [default: None] [required]                                     │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --message  -m      TEXT  A commit message                                                                                     │
│ --help                   Show this message and exit.                                                                          │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

### Usage

```bash
xet rm xet://user/repo/branch/path/to/file/or/dir

# examples
$ xet rm xet://xdssio/titanic/experiment-2/titanic2.csv
Synchronizing with remote
```

## cat (print)

*cat* prints a file to the console.

```bash
Prints a file to stdout

╭─ Arguments ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *    path      TEXT  Source file or folder which will be printed [default: None] [required]                                   │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --limit        INTEGER  Maximum number of bytes to print [default: 0]                                                         │
│ --help                  Show this message and exit.                                                                           │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

### Usage

```bash
xet cat xet://user/repo/branch/path/to/file

# examples
xet cat xet://xdssio/titanic/main/titanic.csv --limit=200
PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
1,0,3,"Braund, Mr. Owen Harris",male,22,1,0,A/5 21171,7.25,,S
2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thaye%   
```

## info

Provide information about a project branch or file.

### Usage

```bash
$ xet info xet://user/repo/branch/path/to/file

# examples
$ xet info xet://xdssio/titanic/main/titanic.csv
-------------------------------  -----  ----
xdssio/titanic/main/titanic.csv  61194  file
-------------------------------  -----  ----
```

## mount

Mount a remote branch to a local directory as read only.   
This is great for data exploration and analysis.

* Mount is lazy and data is not retrieved until it is needed.
* Mount is read only and changes cannot be done to ny mounted file or directory.

```bash 
╭─ Arguments ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *    source      TEXT  Repository and branch of the form xet://user/repo/branch [default: None] [required]                    │
│ *    path        TEXT  Path to mount to. (or a drive letter on windows) [default: None] [required]                            │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

### Usage

```bash
$ xet mount xet://user/repo/branch /path/to/local/dir

## examples
$ xet mount XetHub/Laion400M/main laion
Mounting to "/Users/yonatanalexander/development/xethub/pyxet/laion"
Cloning into temporary directory "/var/folders/gl/cklpy5415rzd6vb8y29rccpr0000gn/T/.tmpYRqYs8"
Mounting as a background task...
```

## branch

The *branch* sub commands let you manage your project branches.

* Deletes a branch. Note that this is not an easily reversible operation.

```bash
╭─ Commands ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ delete    Deletes a branch. Note that this is not an easily reversible operation.                                             │
│ info      Prints information about a branch                                                                                   │
│ ls        list branches of a project.                                                                                         │
│ make      make a new branch copying another branch. Branch names with "/" in them are not supported.                          │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

### Usage

```bash
# Create a new branch
$ xet branch make repo source-branch new-branch
# List branches
$ xet branch ls xet://user/repo
# Delete a branch
$ xet branch delete xet://user/repo/new-branch

# examples
$ xet branch make xet://xdssio/titanic main experiment-3
$ xet branch list xet://xdssio/titanic
name          type
------------  ------
experiment-2  branch
experiment-1  branch
experiment-3  branch
main          branch

$ xet branch delete xet://xdssio/titanic experiment-3 --yes
---------------------------------------------------
                    WARNING
---------------------------------------------------
Branch deletion is not a easily reversible operation
Any data which only exists on a branch will become irreversibly inaccessible

--yes is set. Issuing deletion

$ xet branch info xet://xdssio/titanic main
```

## repo

The *repo* sub commands let you manage your project repositories.

* Deleting a repository is not reversible, you can only do that in the platform:
at https://xethub.com/<user>/<repo-name>/settings.

```bash
╭─ Commands ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ fork                  Forks a new repository from an existing repository.                                                                                                        │
│ info                  provide information on the repo                                                                                                                            │
│ ls                    list repositories of a user.                                                                                                                               │
│ make                  make a new empty public repository.                                                                                                                        │
│ rename                Forks a new repository from an existing repository.                                                                                                        │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

### Usage

```bash 
# Create a new repository
$ xet repo make repo-name
# List repositories
$ xet repo ls xet://user
# fork a repository
$ xet repo fork xet://user/repo-name xet://user/new-repo-name --private
# Rename a repository
$ xet repo rename xet://user/repo-name new-repo-name

examples
xet repo fork xet://xdssio/titanic xet://xdssio/titanic-fork -p
```

## sync

*sync* will copy changed files from source to target similar to `aws s3 sync`. 
* By default, a changed file is one that has a different size between the source and target. If `--use-mtime`
  is provided, then a file whose size is the same will be copied if the modification time for the source is 
  *later* than the target. Note that this flag makes the sync significantly slower.
* Only non-xet sources (e.g. S3 or local filesystem) are allowed.
* Only XetHub targets are allowed (i.e. `xet://<user>/<repo>/<branch>`).
* Modifying source files while a sync is happening has undefined behavior for whether those files copy. 

```bash
╭─ Arguments ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *    source      TEXT  Source folder to sync [default: None] [required]                                                       │
│ *    target      TEXT  Target location of the folder [default: None] [required]                                               │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --use-mtime                   Use mtime as criteria for sync                                                                  │
│ --message    -m      TEXT     A commit message                                                                                │
│ --parallel   -p      INTEGER  Maximum amount of parallelism [default: 32]                                                     │
│ --dryrun                      Displays the operations that would be performed without actually running them.                  │
│ --help                        Show this message and exit.                                                                     │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

### Usage

```bash
# Sync remote S3 bucket to repo
$ xet sync s3://bucket/path/to/source xet://user/repo/branch/path/to/target

# Example sync from S3
$ xet sync s3://my-files xet://XetHub/import-test/my-files
Checking sync
Starting sync
Copying my-files/data.csv to XetHub/import-test/my-files/data.csv...
Copying my-files/data2.csv to XetHub/import-test/my-files/data2.csv...
...
Completed sync. Copied: 20 files, ignored: 277 files

# Example sync from local
$ xet sync . xet://XetHub/import-test/my-local-files
Checking sync
Starting sync
Copying ./dir/data.csv to XetHub/import-test/my-local-files/data.csv...
Copying ./dir/data2.csv to XetHub/import-test/my-local-files/data2.csv...
...
Completed sync. Copied: 53 files, ignored: 130 files
```
