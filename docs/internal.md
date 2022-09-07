## Questions:

* What format is data stored on disk?
* Joins implementation?
* Indexes implementation?
* How do you handle deleted pages (freenode list)?
* How does a WAL work?
* How do you store text with variable lengths?

# NEXT: 

- Remove btree branch factor
- Add column names
- https://stackoverflow.com/questions/40355121/how-does-sqlite-database-stores-tables
- auto-incrementing IDs
- Replace current Datatypes code with record.py. 

## What I've learnt so far:

- Difference between Btrees & B+trees.
- Deleting nodes was trickier than I thought, because you now have any empty page in the middle of file, so you need to store a list of these references to empty pages in the same way you would store a list of references to actual values. 
- How tricky it is to debug things on a bit/byte level. After serialising pages by debugging skills diminish quickly.
- How tricky tree algorithms and recursions are to debug, day to day I'm working with much simpler data structures that are easier to reason about.
- Lexers pretty simple - the most complicated part is managing the cursor. I'm sure I could rewrite it much more simply.
- python unittest framework is pretty feature complete. The only thing I miss from pytest is the pretty output & parameterized tests.
- Didn't realize the VM / parser distinction.
- Didn't have a good grasp of what stats are used in the query planner.

