# toysql

I work with sql databases everyday but don't have deep understanding of how they work, this is an attempt to improve my understanding of their implementation. 

This is a dependency free, minimal clone of sql database written in python. I've intentionally tried to keep the code "simple" ignoring edge cases and optimizations so that anyone can read through and improve their understanding of how databases work.

## Questions:

* What format is data stored on disk?
* Joins implementation?
* Indexes implementation?
* How do you handle deleted pages (freenode list)?

## Inspiration:

- https://cstack.github.io/db_tutorial/
- https://github.com/erikgrinaker/toydb
- https://stackoverflow.com/questions/1108/how-does-database-indexing-work
- https://github.com/NicolasLM/bplustree

## Current Features

1. Fixed table schema. (int, str, str)
2. Can insert rows - always indexed by primary key (int)
3. Can select all rows

TODOs:

- Where filter on pk
- Where filter on other columns
- create table (multiple tables)
- WAL with transactions + rollback
- Indexes (btree not b+tree implementation)
