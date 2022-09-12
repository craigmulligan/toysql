from typing import List, Any, Optional
from toysql.pager import Pager
from toysql.parser import Statement, SelectStatement, InsertStatement, CreateStatement
from enum import Enum, auto
from dataclasses import dataclass


class Opcode(Enum):
    """
    Programs contain a single instance of this opcode as the very first opcode.

    p2 holds the starting instruction address.
    """

    Init = auto()
    """
    OpenRead: Open a read-only cursor for the database table whose root page is P2 in a database file. 
    The database file is determined by P3. P3==0 means the main database,
    P3==1 means the database used for temporary tables, 
    and P3>1 means used the corresponding attached database. 
    Give the new cursor an identifier of P1. 
    The P1 values need not be contiguous but all P1 values should be small integers. 
    It is an error for P1 to be negative.
    """
    OpenRead = auto()
    Rewind = auto()
    Rowid = auto()
    Column = auto()
    ResultRow = auto()
    Next = auto()
    Halt = auto()

    """
    Begin a transaction on database P1 if a transaction is not already active. If P2 is non-zero, then a write-transaction is started, or if a read-transaction is already active, it is upgraded to a write-transaction. If P2 is zero, then a read-transaction is started. If P2 is 2 or more then an exclusive transaction is started.
    P1 is the index of the database file on which the transaction is started. Index 0 is the main database file and index 1 is the file used for temporary tables. Indices of 2 or more are used for attached databases.

    If a write-transaction is started and the Vdbe.usesStmtJournal flag is true (this flag is set if the Vdbe may modify more than one row and may throw an ABORT exception), a statement transaction may also be opened. More specifically, a statement transaction is opened iff the database connection is currently not in autocommit mode, or if there are other active statements. A statement transaction allows the changes made by this VDBE to be rolled back after an error without having to roll back the entire transaction. If no error is encountered, the statement transaction will automatically commit when the VDBE halts.

    If P5!=0 then this opcode also checks the schema cookie against P3 and the schema generation counter against P4. The cookie changes its value whenever the database schema changes. This operation is used to detect when that the cookie has changed and that the current process needs to reread the schema. If the schema cookie in P3 differs from the schema cookie in the database header or if the schema generation counter in P4 differs from the current generation counter, then an SQLITE_SCHEMA error is raised and execution halts. The sqlite3_step() wrapper function might then reprepare the statement and rerun it from the beginning.
    """
    Transaction = auto()
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
    p1: Optional[int] = None
    p2: Optional[int] = None
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


#    registers: List[Any]


class Planner:
    """
    Given a Statement a planner will produce a Program for the VM to execute.

    It's given the schema and the stats table so it can look up info.
    """

    def __init__(self, pager: Pager):
        self.pager = pager

    def get_table_root_page_number(self, tablename: str) -> int:
        return 12

    def plan(self, statements: List[Statement]) -> Program:
        # Initally we assume only one statement.
        [statement] = statements
        program = Program([])

        if isinstance(statement, SelectStatement):
            program.instructions.append(Instruction(Opcode.Init, p2=1))

            root_page_number = self.get_table_root_page_number(
                str(statement._from.value)
            )
            program.instructions.append(
                Instruction(Opcode.OpenRead, p1=0, p2=root_page_number)
            )

        return program
