# toysql

I work with sql databases everyday but I don't have deep understanding of how they work.

This is a dependency free minimalistic clone of sqllite written in python. You should be able to follow the code and improve your understanding of databases.

## Questions:

* What format is data stored on disk?
* Joins implementation?
* Indexes implementation?

## Inspiration:

- https://cstack.github.io/db_tutorial/
- https://github.com/erikgrinaker/toydb
- https://stackoverflow.com/questions/1108/how-does-database-indexing-work

Nextup: 

> Right now, our execute_insert() function always chooses to insert at the end of the table. Instead, we should search the table for the correct place to insert, then insert there. If the key already exists there, return an error.
