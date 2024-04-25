# Git-Xet: Scale Git to Terabyte-sized Repos

Git-Xet is a plugin for git that allows git repositories to scale to terabytes of data.  

## Design Goals

### Content Deduplication

Content -- both binary and larger text-based files -- are deduplicated across commits, files, and blocks of identical content in otherwise different files.   This allows git to efficiently work with repositories containing large datasets that evolve over time.  See our blog post [here](https://about.xethub.com/blog/git-is-for-data-published-in-cidr-2023) for more information on the technical details of this process, including a link to our CIDR 2023 paper.

### Ease of Use

Once a repo is set up to use Git-Xet, nothing more is required. No per-file configuration or tracking is necessary.

### Scalability

Using the `git xet mount` feature, any branch or commit can be mounted locally as a read-only folder.  The data in this folder is seamlessly materialized as needed, allowing large repositories to be viewed quickly.  This is built using our parallel project, [nfsserve](https://github.com/xetdata/nfsserve).

### Flexibility 

Git-Xet naturally integrates seamlessly with [XetHub](https://about.xethub.com/?), which provides a number of nice perks, such as data analytics and integration with tools like [xetcache](https://github.com/xetdata/xetcache) and [pyxet](https://pyxet.readthedocs.io/en/latest/) for easy access to your data from any environment.  It also integrates with GitHub, where repositories can be quickly enabled to use Git-Xet with the [XetData app](https://xethub.com/assets/docs/github-app#xetdata-github-app). Or, just used Git-Xet  locally against a local, private data store.  

### Open Source

Finally, Git-Xet is an open source tool that is free to use and build upon.  By default, the binary data content is stored on XetHub and backed by S3.  It can also be configured against a local data store for a fully free and open source route to managing large repositories.

## Documentation 

Documentation for Git-Xet, PyXet, and the Xet CLI is available on XetHub [here](https://xethub.com/assets/docs/).

## Installation and Development

The easiest way to install Git-Xet is to install one of the prebuilt [binaries](https://github.com/xetdata/xet-tools/releases).  Once the `git-xet` executable is in your path, run `git-xet install` from the command line to configure the git config settings, then any enabled repository will work. 

To build Git-Xet from source, [install the rust toolchain](https://doc.rust-lang.org/cargo/getting-started/installation.html), then run `cargo build --release` in the `rust/` subfolder.  The `git-xet` binary is then in `rust/target/release/`.  After this, run `git-xet install` to set the proper git config settings.

### Get involved

We will develop this package in public under the BSD license and welcome contributions.

Join our [Slack](https://communityinviter.com/apps/xetdata/xet) to get involved. To stay informed about updates, star this repo and sign up at [XetHub](https://xethub.com/user/sign_up) to get the newsletter.


## Optional: XetHub Account Setup

The use of the fully managed [XetHub](about.xethub.com) service provides many perks, including reliable data storage and seamless minimal-configuration integration with git.

### Authentication

Go to [XetHub](https://xethub.com/user/sign_up) to create an account.  On your first sign-in, you should get a personal access token that can be used for authentication, or go to https://xethub.com/user/settings/pat to create a new token. 

There are two ways to authenticate with XetHub:

#### Command Line

Run the command given when you create your personal access token:

```bash
git xet login -e <email> -u <username> -p <personal_access_token>
```
git xet login will write authentication information to `~/.xetconfig`

#### Environment Variables

Environment variables may be sometimes more convenient:

```bash
export XET_USER_EMAIL = <email>
export XET_USER_NAME = <username>
export XET_USER_TOKEN = <personal_access_token>
```

# PyXet - Python SDK for Xet Repositories

<p align="center">
	<img src="https://github.com/xetdata/pyxet/blob/0c7608c97f6a2a0cb2c83dd38fb717913c4d7522/docs/images/logo.png" alt="logo" width="400" />

</p>

[![Version](https://img.shields.io/pypi/v/pyxet.svg?style=flat)](https://pypi.python.org/pypi/pyxet/)
[![Python](https://img.shields.io/pypi/pyversions/pyxet.svg?style=flat)](https://pypi.python.org/pypi/pyxet/)
[![License](https://img.shields.io/github/license/xetdata/pyxet?style=flat)](https://github.com/xetdata/pyxet/blob/main/LICENSE)
[![Downloads](https://img.shields.io/pypi/dm/pyxet?style=flat)](https://pypi.python.org/pypi/pyxet/)
[![Documentation Status](https://readthedocs.org/projects/pyxet/badge/?version=latest)](https://pyxet.readthedocs.io/en/latest/?badge=latest)
[![Discord](https://img.shields.io/discord/1100889165777862807)](https://discord.gg/KCzmjDaDdC)

PyXet is a Python library that provides a pythonic interface for
[XetHub](https://xethub.com/).  

## License

[BSD 3](LICENSE)

## Features

Pyxet has 3 components:

1. A [fsspec](https://filesystem-spec.readthedocs.io)
interface that allows compatible libraries such as Pandas, Polars and Duckdb
to directly access any version of any file in a Xet repository. See below
for some examples.

2. A command line interface inspired by AWSCLI that allows files to be 
uploaded to and downloaded from Xet repository conveniently and efficiently.

3. A file system mount mechanism that allows any version of any Xet repository
to be mounted. This works on Mac, Linux, and Windows 11 Pro.

For API documentation and full examples, please see [here](https://pyxet.readthedocs.io/en/latest/).


## Installation

Set up your virtualenv with:

```sh
$ python -m venv .venv
$ . .venv/bin/activate
```

Then, install pyxet with:

```sh
$ pip install pyxet
```


## Authentication

Signup on [XetHub](https://xethub.com/user/sign_up) and obtain
a username and access token. You should write this down.

There are three ways to authenticate with XetHub:

### Command Line

```bash
xet login -e <email> -u <username> -p <personal_access_token>
```
Xet login will write authentication information to `~/.xetconfig`

### Environment Variable
Environment variables may be sometimes more convenient:
```
export XET_USER_EMAIL = <email>
export XET_USER_NAME = <username>
export XET_USER_TOKEN = <personal_access_token>
```

### In Python
Finally if in a notebook environment, or a non-persistent environment, 
we also provide a method to authenticate directly from Python. Note that
this must be the first thing you run before any other operation:
```python
import pyxet
pyxet.login(<username>, <personal_access_token>, <email>)
```

# Usage

We have, a few basic usage examples here. For complete documentation
please see [here](https://pyxet.readthedocs.io/en/latest/).

Our examples are based on a small Titanic dataset you can see and explore [here](https://xethub.com/xethub/titanic).

## Reading Files and Accessing Repos

A XetHub URL for pyxet is in the form:
```
xet://<repo_owner>/<repo_name>/<branch>/<path_to_file>
```

Reading files from pyxet is easy: `pyxet.open` on a Xet path will return a
python file-like object which you can directly read from.

```python
import pyxet            
print(pyxet.open('xet://XetHub/titanic/main/README.md').readlines())
```


## Pandas Integration

FSSpec integration means that many libraries support reading from Xethub
directly.  For instance: we can easily read the CSV file directly into a Pandas
dataframe:

```python
import pyxet            # make xet:// protocol available
import pandas as pd     # assumes pip install pandas has been run

df = pd.read_csv('xet://XetHub/titanic/main/titanic.csv')
df
```

This should return something like:

```
Out[3]:
     PassengerId  Survived  Pclass  ...     Fare Cabin  Embarked
0              1         0       3  ...   7.2500   NaN         S
1              2         1       1  ...  71.2833   C85         C
2              3         1       3  ...   7.9250   NaN         S
3              4         1       1  ...  53.1000  C123         S
4              5         0       3  ...   8.0500   NaN         S
..           ...       ...     ...  ...      ...   ...       ...
886          887         0       2  ...  13.0000   NaN         S
887          888         1       1  ...  30.0000   B42         S
888          889         0       3  ...  23.4500   NaN         S
889          890         1       1  ...  30.0000  C148         C
890          891         0       3  ...   7.7500   NaN         Q

[891 rows x 12 columns]
```

## Working with a Blob Store

The `XetFS` object in Pyxet implements all the [fsspec](https://filesystem-spec.readthedocs.io/en/latest/)
API For instance, you can list folders with:
```python
fs = pyxet.XetFS()
print(fs.listdir('xethub/titanic/main'))
```

Which should output something like the following:
```
[{'name': 'xethub/titanic/main/.gitattributes', 'size': 79, 'type': 'file'},
{'name': 'xethub/titanic/main/data', 'size': 0, 'type': 'directory'},
{'name': 'xethub/titanic/main/readme.md', 'size': 58, 'type': 'file'},
{'name': 'xethub/titanic/main/titanic.csv', 'size': 61194, 'type': 'file'},
{'name': 'xethub/titanic/main/titanic.json', 'size': 165682, 'type': 'file'},
{'name': 'xethub/titanic/main/titanic.parquet',
'size': 27175,
'type': 'file'}]
```

Here are some other simple ways to access information from an existing repository:

```python
import pyxet

fs = pyxet.XetFS()  # fsspec filesystem

fs.info("xethub/titanic/main/titanic.csv")
# returns repo level info: {'name': 'https://xethub.com/xethub/titanic/titanic.csv', 'size': 61194, 'type': 'file'}

fs.open("xethub/titanic/main/titanic.csv", 'r').read(20)
# returns first 20 characters: 'PassengerId,Survived'

fs.get("xethub/titanic/main/data/", "data", recursive=True)
# download remote directory recursively into a local data folder

fs.ls("xethub/titanic/main/data/", detail=False)
# returns ['data/titanic_0.parquet', 'data/titanic_1.parquet']
```

Pyxet also allows you to write to repositories with Git versioning.

## Writing files with Pyxet

To write files with pyxet, you need to first make a repository you have access to.
An easy thing you can do is to simply fork the titanic repo. You can do so with

```bash
xet repo fork xet://XetHub/titanic
```
(see the Xet CLI documentation below)

This will create a private version of the titanic repository under `xet://<username>/titanic`.

Unlike typical blob stores, XetHub writes are *transactional*. This means the
entire write succeeds, or the entire write fails 
(there is a transaction limit of about 1024 files).

```python
import pyxet
fs = pyxet.XetFS()
user_name = <user_name>
with fs.transaction as tr:
    tr.set_commit_message("hello world")
    f = fs.open(f"{user_name}/titanic/main/hello_world.txt", 'w')
    f.write("hello world")
    f.close()
```

If you navigate to your titanic repository on XetHub, you'll see the new 
`hello_world.txt`.


# Xet CLI
The Xet Command line is the easiest way to interact with a Xet repository.

## Listing and time travel
You can browse the repository with:
```bash
xet ls xet://<username>/titanic/main
```

You can even browse it at any point in history (say 5 minutes ago) with:
```bash
xet ls xet://<username>/titanic/main@{5.minutes.ago}
```

## Downloading
This syntax works everywhere, you can download files with `xet cp`
```bash
# syntax is similar to AWS CLI 
xet cp xet://<username>/titanic/main/<path> <local_path>
xet cp xet://<username>/titanic/main@{5.minutes.ago}/<path> <local_path>
```

And you can also use `xet cp` to upload files:

## Uploading
```bash
xet cp <file/directory> xet://<username>/titanic/main/<path>
```
Of course, you cannot rewrite history, so uploading to `main@{5.minutes.ago}`
is prohibited. 

## Branches
You can easily create branches for collaboration:
```bash
xet branch make xet://<username>/titanic main another_branch
```
This is fast regardless of the size of the repo.

## Copying across repos and branches
Copying across branches are efficient, and can be used to restore a historical
copy of a file which you accidentally overwrote:

```bash
# copying across branch
xet cp xet://<username>/titanic/branch/<file> xet://<username>/titanic/main/<file>
# copying from history to current
xet cp xet://<username>/titanic/main@{5.minutes.ago}/<file> xet://<username>/titanic/main/<file>
```

## S3, GCP, etc
Xet CLI understand every protocol FSSpec does. So all the commands above
work with S3, GCP and many other protocols too. You can also use Xet CLI to
directly upload and download data from S3 to XetHub:
```
$ xet cp xet://... s3://...
$ xet cp s3://... xet://...
```

# Development
See [here](python/pyxet/README.md)

# Encountering Issues?

Please file a bug [here](https://github.com/xetdata/pyxet/issues/new), or
report on our [Discord channel](https://discord.gg/KCzmjDaDdC)! 
We are constant making improvements, especially with
usability and performance.

