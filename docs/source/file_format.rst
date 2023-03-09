===========
File Format
===========

One of the many responsibilities of a database is to take data and write it to non-volitle storage aka disk.
This chapter will take you through the on disk format on toysql. By the end you'll be to construct a database file and
because toysql is compatible with sqlite, read that file back using sqlite as seen in this test.

.. literalinclude:: ../../tests/test_page.py
   :language: python
   :pyobject: test_page_e2e 

.. todo:: 
  
  Overview of file format + diagram

.. include:: datatypes.rst
.. include:: records.rst
.. include:: cells.rst
