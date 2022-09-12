## Questions:

* What format is data stored on disk?
* Joins implementation?
* Indexes implementation?
* How do you handle deleted pages (freenode list)?
* How does a WAL work?
* How do you store text with variable lengths?
* How exactly does the schema cookie work?
* If everything is handled by the VM eg opening btree cursors loading values etc. How does the planner (eg pre-vm look up the schema & stats table?) Are those tables looked up directly, or do they still use the vm internally? I'm guessing this has something to do with the schema cookie. [relevant](https://github.com/sqlite/sqlite/blob/41ce47c4f4bcae3882fdccec18a6100a85f4bba5/src/prepare.c#L710) looks like it runs it through [`sqlite3_exec`](https://github.com/sqlite/sqlite/blob/master/src/legacy.c) which does run it through `sqlite3_prepare_v2` the planner & `sqlite3_step` the vm.

## What I've learnt so far:

- Difference between Btrees & B+trees.
- Deleting nodes was trickier than I thought, because you now have any empty page in the middle of file, so you need to store a list of these references to empty pages in the same way you would store a list of references to actual values. 
- How tricky it is to debug things on a bit/byte level. After serialising pages by debugging skills diminish quickly.
- How tricky tree algorithms and recursions are to debug, day to day I'm working with much simpler data structures that are easier to reason about.
- Lexers pretty simple - the most complicated part is managing the cursor. I'm sure I could rewrite it much more simply.
- python unittest framework is pretty feature complete. The only thing I miss from pytest is the pretty output & parameterized tests.
- Didn't realize the VM / parser distinction.
- Didn't have a good grasp of what stats are used in the query planner.

# String lengths 
SQLite does let you write VARCHAR(255) or NVARCHAR(255), but the only relevant part of that is CHAR (which gives the column "text affinity"). The number in parentheses is allowed for compatibility with standard SQL, but is ignored.

In SQLite, there's little need for length constraints on strings, because (1) it doesn't affect the amount of space the string takes up on disk and (2) there's no arbitrary limit (except for SQLITE_MAX_LENGTH) on the length of strings that can be used in an INDEX.

If you have an actual need to place a limit on the number of characters in a column, just add a constraint like CHECK(LENGTH(TheColumn) <= 255)

- https://stackoverflow.com/questions/40355121/how-does-sqlite-database-stores-tables
