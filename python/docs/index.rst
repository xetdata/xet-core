.. image:: images/logo.png
  :width: 250
  :alt: pyxet logo

Pyxet Documentation
===================

pyxet is a Python library that provides a pythonic interface for
`XetHub <https://xethub.com/>`_.  Xethub is simple git-based system capable of
storing TBs of ML data and models in a single repository, with block-level 
data deduplication that enables hundreds of versions of similar data to be
stored without requiring much storage. 

Join our `Discord <https://discord.gg/KCzmjDaDdC>`_ to get involved. 
To stay informed about updates, star this repo and sign up for 
`XetHub <https://xethub.com/user/sign_up>`_ to get the newsletter.

Features
--------

pyxet provides 3 components:

1. A `fsspec <https://filesystem-spec.readthedocs.io>`_
interface that allows compatible libraries such as Pandas, Polars and Duckdb
to directly access any version of any file in a Xet repository. See below
for some examples.

2. A command line interface inspired by AWSCLI that allows files to be 
uploaded to and downloaded from Xet repository conveniently and efficiently.

3. A file system mount mechanism that allows any version of any Xet repository
to be mounted. This works on Mac, Linux, and Windows 11 Pro.


Installation
------------

The easiest to authenticate is to signup on `XetHub <https://xethub.com/user/sign_up>`_ and obtain
a username and access token. You should write this down.

Set up your virtualenv with:

```sh
$ python -m venv .venv
$ . .venv/bin/activate
```

Then, install pyxet with:

```sh
$ pip install pyxet
```


Authentication
--------------

There are three ways to authenticate with XetHub:

Command Line
~~~~~~~~~~~~

.. code-block:: bash

    xet login -e <email> -u <username> -p <personal_access_token>

Xet login will write to authentication information to `~/.xetconfig`

Environment Variable
~~~~~~~~~~~~~~~~~~~~
Environment variables may be sometimes more convenient:

.. code-block:: bash

    export XET_USER_EMAIL = <email>
    export XET_USER_NAME = <username>
    export XET_USER_TOKEN = <personal_access_token>

In Python
~~~~~~~~~
Finally if in a notebook environment, or a non-persistent environment, 
we also provide a method to authenticate directly from Python. Note that
this must be the first thing you run before any other operation:

.. code-block:: python

    import pyxet
    pyxet.login(<username>, <personal_access_token>, <email>)

Quickstart
----------

Read a CSV file:

.. code-block:: python

    import pyxet            # make xet:// protocol available
    import pandas as pd     # assumes pip install pandas has been run

    df = pd.read_csv('xet://XetHub/titanic/main/titanic.csv')

Checkout the rest of the documentation for detailed usage examples!

Encountering Issues?
====================

Please file a bug `here <https://github.com/xetdata/pyxet/issues/new>`_, or
report on our `Discord channel <https://discord.gg/KCzmjDaDdC>`_! 
We are constant making improvements, especially with
usability and performance.

---------------------------------------

.. toctree::
   :maxdepth: 2
   :caption: Introduction

   markdowns/quickstart
   markdowns/writing

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   markdowns/filesystem
   markdowns/cli
   markdowns/mount

.. toctree::
   :maxdepth: 2
   :caption: Use Cases

   markdowns/collaboration
   markdowns/model_versioning

.. toctree::
   :maxdepth: 2
   :caption: API Documentation

   pyxet

