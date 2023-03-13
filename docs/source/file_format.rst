===========
File Format
===========


One of the responsibilities of a database is to read and write data to disk. When reading and writing,
the database needs to serialize and deserialize this data correctly to ensure is not corrupted.

This chapter will take you through the on disk format of toysql. By the end you'll be to construct a database file and
because toysql fileformat is a subset of sqlite, read that file back using sqlite as seen in this test.



.. literalinclude:: ../../tests/test_page.py
   :language: python
   :pyobject: test_page_e2e 


########################
A quick primer on BTrees
########################

We will tackle b-trees in depth in the :ref:`next chapter<btrees_chapter>` but for now it might be helpful to get a fundemental understanding. 
A B+Tree is tree datastructure which is great for storing values by a key. 
It has internal nodes which store ordered key ranges which point to child nodes eventually pointing to leaf nodes which store the key value pairs. 

The fileformat documents how to store these Btrees physically on disk.    

.. todo:: 
  
  Overview of file format + diagram

.. include:: datatypes.rst
.. include:: records.rst
.. include:: cells.rst
