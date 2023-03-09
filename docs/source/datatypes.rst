##########
datatypes
##########

The smallest component of the on-disk format is datatypes. These are things like :code:`Integer`, :code:`Text`, :code:`Null`. They are used both to store user generated data as well as internal metadata. We are going to implement each type needed to implement records, cells and pages. 

.. TODO::

   I think we need to give a brief introduction to the logical layout here and how btrees work so that the summaries make sense. See chidb for inspo.

* Datatype - smallest unit of storage responsible for individual values like text, integer etc.   
* Record - Stores each table record equivalent to a database row.
* Cell - There are two types of cells. A LeafPageCell which stores a record. And a InteriorPageCell which stores pointers to other pages. The difference will be covered in depth in (LINK to section in Btree chapter).
* Page - A Page is made up cells. Again there are two types of pages ones that store records LeafPages and ones that store InteriorPages.
* A file - A File is made up of consecutive pages.

Integers
--------

At the end of this chapter we will have built out a number of integer types which can serialize integers into hexadecimal bytes for on disk storage. SQLite uses a number of different integer types from fixed unsigned integers to unsigned to variable lengths. 

Fixed Width Integers
^^^^^^^^^^^^^^^^^^^^

The first types of integer types we are going to implement are fixed width meaning they will always be :code:`n` bits wide/long. For instance a :code:`Int8` will always be 8 bits wide. 
For instance the integer 5 as an 8 bit integer represented in binary is :code:`00000101` and :code:`\x05` in hexadecimal. 

Let's start with a test to illustrate the functionality we are after:

.. literalinclude:: ../../tests/test_datatypes.py
   :language: python
   :pyobject: TestGroupFixedInt
   :lines: 1-7

.. note::
  Take special note of this Int8 interface. Every Datatype, Record Cell, Page will implement the :code:`.from_bytes`, and :code:`.to_bytes` methods. 

Lucky for us python's standard library gives us all the tools we need to implement these methods. The following code  
will serialize an integer to and from hexadecimal bytes. We have specified that we want the bit order to be big endian [#]_ and unsigned [#]_

.. code-block:: python

  class Int8:
      @staticmethod
      def to_bytes(i: int) -> bytes:

          # Note we are dividing the 8 bits here by 8 because  
          return (i).to_bytes(8 // 8, byteorder="big", signed=False)

      @staticmethod
      def from_bytes(buffer: BytesIO) -> int:

          # Note we are dividing the 8 bits here by 8 because  
          return int.from_bytes(buffer.read(8 // 8), "big")

Your tests should now be passing. Let's go ahead and add tests for the rest of the Fixed Width integers that we'll need.   

.. literalinclude:: ../../tests/test_datatypes.py
   :language: python
   :pyobject: TestGroupFixedInt


Now let's modify our `Int8` class so that we can reuse it for the remainder of the classes. 

.. literalinclude:: ../../toysql/datatypes.py
   :language: python
   :pyobject: FixedInt

Now we can inherit this class and define our classes by just setting the :code:`width` and :code:`signed` flags. 

.. literalinclude:: ../../toysql/datatypes.py
   :language: python
   :pyobject: Int8

An unsigned integer would look like this.  

.. literalinclude:: ../../toysql/datatypes.py
   :language: python
   :pyobject: UInt32


Go ahead and add the remaining classes :code:`Int8`, :code:`Int16`, :code:`Int32`, :code:`UInt8`, :code:`UInt16`, :code:`Uint32`. If all your tests are passing, you can move on to implementing `Variable Width Integers`_

Variable Width Integers
^^^^^^^^^^^^^^^^^^^^^^^^

If you have used SQLite before you may have noticed that you never have to define an integers width when declaring schema.
For instance when creating a table you may write. 

.. code-block:: sql 

  CREATE TABLE products(code INTEGER PRIMARY KEY,
    name TEXT, price INTEGER)
   
Instead of explicitly defining the integer size for :code:`price`.

.. code-block:: sql

  CREATE TABLE products(code INTEGER PRIMARY KEY, 
    name TEXT, price INTEGER(8))

.. note::
  To conform with other SQL databases sqlite will happily accept :code:`price INTEGER(8)` but it will ignore the 8 and store it the same as any other integer.

This is because sqlite used variable width integer encoding for all it's integers. This means that the database can pick the appropriate integer width which saves disk space as you don't need to use a 32 bit integer when a 8 bit one will do. Variable width Integers or VarInts are conceptually fairly simple, you split the binary representation into 7 bit chunks and use the last (or high) bit as a flag to indicate if there are more bytes to be read.

So if we wanted to represent :code:`1,000`, we start with its binary representation split into 7 bit chunks:

.. TODO::  
   Give an example of different sizes and their maximum integer sizes.

.. code-block:: 

  0000111 1101000


Then we add a "1" flag bit to the beginning of the first chunk to indicate we have more chunks, and a "0" flag bit to the beginning of the second chunk to indicate we don't have any more chunks:

.. code-block::

  10000111 01101000

.. note::
  This example is pulled directly from a great write up by Fly.io on varints. [#]_

To simplify our implementation, we are only going to implement the varint encoding but not dynamically choosing the correct bit length. This means we'll use more disk space that strictly necesscary but our disk format will still be compatible with sqlite. 

As always let's start off with a test using our example above.

.. literalinclude:: ../../tests/test_datatypes.py
   :dedent: 4
   :language: python
   :pyobject: TestGroupVarInt.test_varint_encode

Text
----

Some text stuff

.. [#] Byte order determines if the leading or the trailing bits are more significant. For instance an 8 bit integer with a value of 5 with :code:`big` endian byte ordering will be :code:`00000101`, but in :code:`little` endian ordering it will be reversed as :code:`10100000` 
.. [#] A signed integer will use one of the bits to store the sign.
.. [#] https://fly.io/blog/sqlite-internals-btree/ 
