# toysql

I work with sql databases everyday but I don't have deep understanding of how they work.

This is a dependency free, minimal clone of sql written in python. You should be able to follow the code and improve your understanding of databases.

## Questions:

* What format is data stored on disk?
* Joins implementation?
* Indexes implementation?

## Inspiration:

- https://cstack.github.io/db_tutorial/
- https://github.com/erikgrinaker/toydb
- https://stackoverflow.com/questions/1108/how-does-database-indexing-work
- https://github.com/NicolasLM/bplustree

Next steps:

1. Store tree to disk with Pager.

index = 1

parent:
  keys: [6] 
  values: [1, 2]

child:
  keys: [12]
  values: [3, 4]


parent: 
  keys: [6, 12]
  values: [1, 3, 4] 

child:
  keys: [12]
  values: [3, 4]


-----------

parent:
  keys: [1]
  values: [1, 2] 

child: 
  keys: [2]
  values: [3, 4]

after merge:

parent: 
  keys: [1, 2]
  values: [1, 3, 4] 

child:
  keys: [2]
  values: [3, 4]


--- Desired:
parent:
  keys: [1]
  values: [1, 2] 

child: 
  keys: [2]
  values: [3, 4]

->

parent:
  keys: [1, 2]
  values: [1, 3, 4]
