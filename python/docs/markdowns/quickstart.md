# Quickstart

[XetHub](https://xethub.com/) is a cloud storage platform with Git capabilities. It is a great place to store your data,
models,
logs, and code with versioning. The pyxet library allows you to easily access XetHub files directly from Python.

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

(accountsetup)=
## Authentication

Signup on [XetHub](https://xethub.com/user/sign_up) and obtain
a username and personal access token. You should write this down.

There are three ways to authenticate with XetHub:

### Command Line

```bash
xet login -e <email> -u <username> -p <personal_access_token>
```
Xet login will write to authentication information to `~/.xetconfig`

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

## Demo

To verify that pyxet is working, let's load a CSV file directly into a Pandas dataframe, leveraging pyxet's support for
Python fsspec.

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

pyxet provides a Python SDK (with a CLI on the way!) for interacting with XetHub repositories as blob stores, while 
leveraging the power of Git branches and versioning.

A XetHub URL for pyxet is in the form:
```
xet://<repo_owner>/<repo_name>/<branch>/<path_to_file>
```

Unlike with traditional blob stores, the ability to call a branch means that you can choose whether to 
use the most recent version of a file/directory or to reference a particular branch or commit.

To work with a repository as a file system, use `pyxet.XetFS`, which implements [fsspec](https://filesystem-spec.readthedocs.io/en/latest/)
Here are some simple ways to access information from an existing repository:

```python
import pyxet

fs = pyxet.XetFS()  # fsspec filesystem

fs.info("XetHub/titanic/main/titanic.csv")  
# returns repo level info: {'name': 'XetHub/main/titanic.csv', 'size': 61194, 'type': 'file'}

fs.open("XetHub/titanic/main/titanic.csv", 'r').read(20)
# returns first 20 characters: 'PassengerId,Survived'

fs.get("XetHub/titanic/main/data/", "data", recursive=True)  
# download remote directory recursively into a local data folder

fs.ls("XetHub/titanic/main/data/", detail=False)  
# returns ['data/titanic_0.parquet', 'data/titanic_1.parquet']
```

Pyxet also allows you to write to repositories with Git versioning. 
