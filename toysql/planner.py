from typing import List, Any, Optional, Callable
from toysql.pager import Pager
from toysql.parser import (
    Statement,
    SelectStatement,
    InsertStatement,
    CreateStatement,
    Parser,
)
from toysql.lexer import StatementLexer
from enum import Enum, auto
from dataclasses import dataclass
from toysql.exceptions import TableFoundException


class Opcode(Enum):
    """
    Init: Programs contain a single instance of this opcode as the very first opcode.

    p2 holds the starting instruction address.
    """

    Init = auto()
    """
    OpenRead: Open a read-only cursor for the 
    database table whose root page is P2 in a database file. 

    The database file is determined by P3. P3==0 means the main database,
    P3==1 means the database used for temporary tables, 

    and P3>1 means used the corresponding attached database. 
    Give the new cursor an identifier of P1. 
    The P1 values need not be contiguous but all P1 values should be small integers. 
    It is an error for P1 to be negative.
    """
    OpenRead = auto()
    """
    Rewind: The next use of the Rowid or Column or Next instruction for P1 will refer to the first entry in the database table or index. If the table or index is empty, jump immediately to P2. If the table or index is not empty, fall through to the following instruction.
    """
    Rewind = auto()
    """
    Rowid: Store in register P2 an integer which is the key of the table entry that P1 is currently point to.
    P1 can be either an ordinary table or a virtual table. There used to be a separate OP_VRowid opcode for use with virtual tables, but this one opcode now works for both table types.
    """
    Rowid = auto()
    """
    Column: Interpret the data that cursor P1 points to as a structure built using the MakeRecord instruction. (See the MakeRecord opcode for additional information about the format of the data.) Extract the P2-th column from this record. If there are less that (P2+1) values in the record, extract a NULL.
    The value extracted is stored in register P3.

    If the record contains fewer than P2 fields, then extract a NULL. Or, if the P4 argument is a P4_MEM use the value of the P4 argument as the result.

    If the OPFLAG_LENGTHARG and OPFLAG_TYPEOFARG bits are set on P5 then the result is guaranteed to only be used as the argument of a length() or typeof() function, respectively. The loading of large blobs can be skipped for length() and all content loading can be skipped for typeof().
    """
    Column = auto()
    """
    The registers P1 through P1+P2-1 contain a single row of results. This opcode causes the sqlite3_step() call to terminate with an SQLITE_ROW return code and it sets up the sqlite3_stmt structure to provide access to the r(P1)..r(P1+P2-1) values as the result row.
    """
    ResultRow = auto()
    """
    Advance cursor P1 so that it points to the next key/data pair in its table or index. If there are no more key/value pairs then fall through to the following instruction. But if the cursor advance was successful, jump immediately to P2.
    The Next opcode is only valid following an SeekGT, SeekGE, or Rewind opcode used to position the cursor. Next is not allowed to follow SeekLT, SeekLE, or Last.

    The P1 cursor must be for a real table, not a pseudo-table. P1 must have been opened prior to this opcode or the program will segfault.

    The P3 value is a hint to the btree implementation. If P3==1, that means P1 is an SQL index and that this instruction could have been omitted if that index had been unique. P3 is usually 0. P3 is always either 0 or 1.

    If P5 is positive and the jump is taken, then event counter number P5-1 in the prepared statement is incremented.
    """
    Next = auto()
    """
    Halt:  Exit immediately. All open cursors, etc are closed automatically.

    P1 is the result code returned by sqlite3_exec(), sqlite3_reset(), or sqlite3_finalize(). For a normal halt, this should be SQLITE_OK (0). For errors, it can be some other value. If P1!=0 then P2 will determine whether or not to rollback the current transaction. Do not rollback if P2==OE_Fail. Do the rollback if P2==OE_Rollback. If P2==OE_Abort, then back out all changes that have occurred during this execution of the VDBE, but do not rollback the transaction.

    If P4 is not null then it is an error message string.

    P5 is a value between 0 and 4, inclusive, that modifies the P4 string.

    0: (no change) 1: NOT NULL contraint failed: P4 2: UNIQUE constraint failed: P4 3: CHECK constraint failed: P4 4: FOREIGN KEY constraint failed: P4

    If P5 is not zero and P4 is NULL, then everything after the ":" is omitted.

    There is an implied "Halt 0 0 0" instruction inserted at the very end of every program. So a jump past the last instruction of the program is the same as executing Halt.
    """

    Halt = auto()

    """
    Transaction: Begin a transaction on database P1 if a transaction is not already active. If P2 is non-zero, then a write-transaction is started, or if a read-transaction is already active, it is upgraded to a write-transaction. If P2 is zero, then a read-transaction is started. If P2 is 2 or more then an exclusive transaction is started.
    P1 is the index of the database file on which the transaction is started. Index 0 is the main database file and index 1 is the file used for temporary tables. Indices of 2 or more are used for attached databases.

    If a write-transaction is started and the Vdbe.usesStmtJournal flag is true (this flag is set if the Vdbe may modify more than one row and may throw an ABORT exception), a statement transaction may also be opened. More specifically, a statement transaction is opened iff the database connection is currently not in autocommit mode, or if there are other active statements. A statement transaction allows the changes made by this VDBE to be rolled back after an error without having to roll back the entire transaction. If no error is encountered, the statement transaction will automatically commit when the VDBE halts.

    If P5!=0 then this opcode also checks the schema cookie against P3 and the schema generation counter against P4. The cookie changes its value whenever the database schema changes. This operation is used to detect when that the cookie has changed and that the current process needs to reread the schema. If the schema cookie in P3 differs from the schema cookie in the database header or if the schema generation counter in P4 differs from the current generation counter, then an SQLITE_SCHEMA error is raised and execution halts. The sqlite3_step() wrapper function might then reprepare the statement and rerun it from the beginning.
    """
    Transaction = auto()
    """
    Goto: An unconditional jump to address P2. The next instruction executed will be the one at index P2 from the beginning of the program.

    The P1 parameter is not actually used by this opcode. However, it is sometimes set to 1 instead of 0 as a hint to the command-line shell that this Goto is the bottom of a loop and that the lines from P2 down to the current line should be indented for EXPLAIN output.
    """
    Goto = auto()


@dataclass
class Instruction:
    """
    Each instruction has an opcode and five operands named P1, P2 P3, P4, and P5

    The P1, P2, and P3 operands are 32-bit signed integers.

    These operands often refer to registers.

    For instructions that operate on b-tree cursors,
    the P1 operand is usually the cursor number.

    For jump instructions,
    P2 is usually the jump destination.

    P4 may be a 32-bit signed integer,

    a 64-bit signed integer,
    a 64-bit floating point value,
    a string literal,
    a Blob literal,
    a pointer to a collating sequence comparison function,
    or a pointer to the implementation of an application-defined SQL function,
    or various other things.

    P5 is an 16-bit unsigned integer normally used to hold flags.
    Bits of the P5 flag can sometimes affect the opcode in subtle ways.

    For example, if the SQLITE_NULLEQ (0x0080) bit of the P5 operand is set on the Eq opcode,
    then the NULL values compare equal to one another.
    Otherwise NULL values compare different from one another.

    Some opcodes use all five operands. Some opcodes use one or two. Some opcodes use none of the operands.
    """

    opcode: Opcode
    p1: Optional[Any] = None
    p2: Optional[Any] = None
    p3: Optional[int] = None
    p4: Optional[Any] = None  # TODO narrow type
    p5: Optional[int] = None


@dataclass
class Program:
    """
    Every bytecode program has a fixed (but potentially large) number of registers. A single register can hold a variety of objects:

    A NULL value
    A signed 64-bit integer
    An IEEE double-precision (64-bit) floating point number
    An arbitrary length strings
    An arbitrary length BLOB
    A RowSet object (See the RowSetAdd, RowSetRead, and RowSetTest opcodes)
    A Frame object (Used by subprograms - see Program)
    A register can also be "Undefined" meaning that it holds no value at all. Undefined is different from NULL. Depending on compile-time options, an attempt to read an undefined register will usually cause a run-time error. If the code generator (sqlite3_prepare_v2()) ever generates a prepared statement that reads an Undefined register, that is a bug in the code generator.

    Registers are numbered beginning with 0. Most opcodes refer to at least one register.

    The number of registers in a single prepared statement is fixed at compile-time.
    """

    instructions: List[Instruction]

    def resolve_refs(self):
        """
        TODO probably a better data structure for this.
        """
        for instruction in self.instructions:
            if isinstance(instruction.p1, Instruction):
                instruction.p1 = self.instructions.index(instruction.p1)

            if isinstance(instruction.p2, Instruction):
                instruction.p2 = self.instructions.index(instruction.p2)


SCHEMA_TABLE_NAME = "schema"
SCHEMA_TABLE_SQL_TEXT = f"CREATE TABLE {SCHEMA_TABLE_NAME} (id INT, name text(12), sql_text text(500), root_page_number INT);"


class Planner:
    """
    Given a Statement a planner will produce a Program for the VM to execute.

    It's given the schema and the stats table so it can look up info.
    """

    def __init__(self, pager: Pager, get_schema: Callable[[], List[List[Any]]]):
        self.pager = pager

        # TODO sqlite uses a schema cookie to cache schemas
        # Not sure if we want to add that complexity
        self.get_schema = get_schema

        # These are needed to parse schema_table.sql_text
        # values to interpret column names and types
        self.lexer = StatementLexer()
        self.parser = Parser()

    def parse_input(self, sql_text: str):
        tokens = self.lexer.lex(sql_text)
        stmts = self.parser.parse(tokens)
        return stmts

    def get_column_names_from_sql_text(self, sql_text: str):
        [statement] = self.parse_input(sql_text)
        names = []
        for col in statement.items:
            if col.name.value != "*":
                names.append(col.name.value)

        return names

    def get_table_column_names(self, table_name):
        if table_name == SCHEMA_TABLE_NAME:
            return self.get_column_names_from_sql_text(SCHEMA_TABLE_SQL_TEXT)

        for values in self.get_schema():
            if values[1] == table_name:
                sql_text = values[2]
                return self.get_column_names_from_sql_text(sql_text)

        raise TableFoundException(f"Table: {table_name} not found")

    def get_table_root_page_number(self, table_name: str) -> int:
        # We don't need the schema here.
        if table_name == SCHEMA_TABLE_NAME:
            return 0

        for record in self.get_schema():
            if record[1] == table_name:
                root_page_number = record[3]
                return root_page_number

        raise TableFoundException(f"Table: {table_name} not found")

    def plan(self, statements: List[Statement]) -> Program:
        # Initally we assume only one statement.
        [statement] = statements
        program = Program([])

        if isinstance(statement, SelectStatement):
            table_page_number = self.get_table_root_page_number(
                str(statement._from.value)
            )
            transaction = Instruction(Opcode.Transaction, p1=0, p2=0, p3=21)
            init = Instruction(Opcode.Init, p2=transaction)
            rewind = Instruction(Opcode.Rewind, p1=0, p2=7, p3=0)
            open_read = Instruction(
                Opcode.OpenRead, p1=0, p2=table_page_number, p3=0, p4=2
            )
            goto = Instruction(Opcode.Goto, p1=0, p2=open_read, p3=0)

            program.instructions = [
                init,
                open_read,
                rewind,
                Instruction(Opcode.Rowid, p1=0, p2=1, p3=0),
                Instruction(Opcode.Column, p1=0, p2=1, p3=2),
                Instruction(Opcode.ResultRow, p1=1, p2=2, p3=0),
                Instruction(Opcode.Next, p1=0, p2=3, p3=0, p5=1),
                Instruction(Opcode.Halt, p1=0, p2=0, p3=0),
                transaction,
                goto,
            ]

        program.resolve_refs()

        return program
