pyxet module
============

Login
-----
.. automethod:: pyxet::login


Open
----
.. automethod:: pyxet::open



XetFS
-----
.. autoclass:: pyxet::XetFS
   :special-members: 
   :members: 
   :exclude-members: set_repo_attr,rename_repo, duplicate_repo, fork_repo, make_repo, unstrip_protocol
   :show-inheritance:

   **Inherited Members**

    .. automethod:: cat
    .. automethod:: cat_file
    .. automethod:: checksum
    .. automethod:: copy
    .. automethod:: delete
    .. automethod:: download
    .. automethod:: get
    .. automethod:: get_file
    .. automethod:: glob
    .. automethod:: head
    .. automethod:: isfile
    .. automethod:: put

MultiCommitTransaction
----------------------
.. autoclass:: pyxet::MultiCommitTransaction
   :members: complete, copy, mv, open_for_write, rm, set_commit_message
