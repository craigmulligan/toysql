##########
datatypes
##########

The smallest component of the on-disk format is datatypes. These are things like :code:`Integer`, :code:`Text`, :code:`Null`. They are used both to store user generated data as well as internal metadata. We are going to implement each type needed to implement records, cells and pages. 

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
   :pyobject: FixedIntTestGroup
   :lines: 1-7

.. note::
  Take special note of this Int8 interface. Every Datatype, Record Cell, Page will implement the :code:`.from_bytes`, and :code:`.to_bytes` methods. 

Luck for us python's standard library gives us all the tools we need to implement these methods. The following code  
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
   :pyobject: FixedIntTestGroup


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
For instance creating a table you may write. 

.. code-block:: sql 

  CREATE TABLE products(code INTEGER PRIMARY KEY,
    name TEXT, price INTEGER)
   
Instead of explicitly defining the integer size for price.

.. code-block:: sql

  CREATE TABLE products(code INTEGER PRIMARY KEY, 
    name TEXT, price INTEGER(8))

.. note::
  To conform with other sql engines sqlite will happily accept :code:`price INTEGER(8)` but it will ignore the 8 and store it the same as any other integer.

This is because sqlite used variable width integer encoding for all user stored integers. This means that it will always pick the correct width for the integer that needs to be stored.   


Text
----

Some text stuff

.. [#] Byte order determines if the leading or the trailing bits are more significant. For instance an 8 bit integer with a value of 5 with :code:`big` endian byte ordering will be :code:`00000101`, but in :code:`little` endian ordering it will be reversed as :code:`10100000` 
.. [#] A signed integer will use one of the bits to store the sign.
