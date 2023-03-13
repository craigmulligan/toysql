Toysql
======

############
Introduction
############

I work with SQL databases everyday but I never had have solid understanding of how they work. I never knew the difference between a BTree and a B+tree, 
or understood how they store variable length integers and text or that most databases implement a virtual machine. Through this book I'll implement a very simple
clone of sqlite and cover topics like lexing, parsing, VMs, tree algorithms and serialization formats.

################
Who is this for?
################

I'd suggest only reading this book if you have at least a year of programming expierence. 
I've tried to make the content very simple and approachable but I won't go over standard progamming concepts.
All the code is written in python using just the standard library but you should be able to follow along in your language of choice. 


.. toctree::
  :numbered:

  file_format.rst 
  btrees.rst


.. * [Compiler](/compiler)
.. * [VM](/vm)

.. * [Lexer](/lexer)
.. * [Parser](/parser)
.. * [E2E](/e2e)
