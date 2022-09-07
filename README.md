# toysql [WIP]

I work with sql databases everyday but don't have deep understanding of how they work, this is an attempt to improve my understanding of their implementation. 

This is a dependency free, minimal clone of sql database written in python. I've intentionally tried to keep the code "simple" ignoring edge cases and optimizations so that anyone can read through and improve their understanding of how databases work.

## Reading:

- https://www.sqlite.org/fileformat.html
- https://cstack.github.io/db_tutorial/
- https://github.com/erikgrinaker/toydb
- https://stackoverflow.com/questions/1108/how-does-database-indexing-work
- https://github.com/NicolasLM/bplustree
- https://notes.eatonphil.com/database-basics.html
- https://jvns.ca/blog/2014/09/27/how-does-sqlite-work-part-1-pages/
- https://jvns.ca/blog/2014/10/02/how-does-sqlite-work-part-2-btrees/
- https://books.google.com.mx/books?id=9Z6IQQnX1JEC&lpg=PP1&pg=PA185#v=onepage&q&f=false
- https://fly.io/blog/sqlite-internals-btree/
- https://fly.io/blog/sqlite-internals-rollback-journal/
- https://www.geeksforgeeks.org/introduction-of-b-tree
- https://www.cs.nmsu.edu/~hcao/teaching/cs582/note/DB2_4_BplusTreeExample.pdf
- https://www.programiz.com/dsa/b-plus-tree#:~:text=A%20B%2B%20tree%20is%20an,the%20data%20easier%20and%20faster.

## Current Features

1. Fixed table schema. (int, str, str) # TODO remove.
2. Can insert rows - always indexed by primary key (int)
3. Can select all rows

What I've learnt so far:

- Difference between Btrees & B+trees.
- Deleting nodes was trickier than I thought, because you now have any empty page in the middle of file, so you need to store a list of these references to empty pages in the same way you would store a list of references to actual values. 
- How tricky it is to debug things on a bit/byte level. After serialising pages by debugging skills diminish quickly.
- How tricky tree algorithms and recursions are to debug, day to day I'm working with much simpler data structures that are easier to reason about.
- Lexers pretty simple - the most complicated part is managing the cursor. I'm sure I could rewrite it much more simply.
- python unittest framework is pretty feature complete. The only thing I miss from pytest is the pretty output & parameterized tests.
- Didn't realize the VM / parser distinction.
- Didn't have a good grasp of what stats are used in the query planner.

## Questions:

* What format is data stored on disk?
* Joins implementation?
* Indexes implementation?
* How do you handle deleted pages (freenode list)?
* How does a WAL work?
* How do you store text with variable lengths?

# NEXT: 

- Replace current Datatypes code with record.py. 
- https://stackoverflow.com/questions/40355121/how-does-sqlite-database-stores-tables
- auto-incrementing IDs
