Toysql
======

############
Introduction
############

I work with SQL databases everyday but I've always had a bit of a handwavey understanding of them. I knew for instance that there was a tree like datastructure called a btree used to make lookups fast but I didn't know the difference between a B+tree or a Btree or understand how variable length text and integers were stored or that databases have their own VM with opcodes like :code:`CreateTable`.  

To understand databases a little better I set out to build a very simple one and wrote this book for anyone who might want to do the same. It's the book I wished I had a year ago.

Through this book we'll implement a very simple clone of sqlite in python and cover topics like lexing, parsing, VMs, btree algorithms and serialization formats. We'll follow a test-driven approach.


################
Who is this for?
################

This book is for database users who would like to improve their understanding of database internals. While I've tried to make the content easy to understand, I won't cover standard programming concepts like loops, classes & scope. So I'd only recomend this book if you have been programming and working with databases for atleast a few months. Prior expierence with python is helpful but not a requirement as you should be able to pick it up very easily coming from almost any other language.
 
#################
Acknowledgements
#################

The structure of this book follows the `chidb course <chidb_>`_ by The University of Chicago. It's been an invaluable resource as I've used some of there test files to implement our own tests and their documentation to inform my own explanations. 

I've also made heavy use of the `sqlite <flyio_sqlite_vm_>`_ `internals <flyio_sqlite_pages_>`_ series by `fly.io <flyio_>`_.



###############
Contributing
###############

If you have any feedback or edits to make please file and issue or a PR on `github <github_>`_ 

###############
Contents
###############

.. toctree::
  :numbered:

  file_format.rst 
  btrees.rst


.. * [Compiler](/compiler)
.. * [VM](/vm)

.. * [Lexer](/lexer)
.. * [Parser](/parser)
.. * [E2E](/e2e)

.. _chidb: http://chi.cs.uchicago.edu/chidb/index.html
.. _flyio_sqlite_vm: https://fly.io/blog/sqlite-virtual-machine/
.. _flyio_sqlite_pages: https://fly.io/blog/sqlite-internals-btree/
.. _flyio: https://fly.io
.. _github: https://github.com/craigmulligan/toysql

