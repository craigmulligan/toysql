from typing import List, Any, Optional
from toysql.pager import Pager
from toysql.parser import (
    SelectStatement,
    InsertStatement,
    CreateStatement,
    Parser,
)
from toysql.lexer import StatementLexer, Kind
from enum import Enum, auto
from dataclasses import dataclass
from toysql.exceptions import TableFoundException


"""
All opcodes can be found here: https://www.sqlite.org/opcode.html
This is a subset of them implemented for toysqls feature set.
"""


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
    CreateBtree: Allocate a new b-tree in the main database file if P1==0 or in the TEMP database file if P1==1 or in an attached database if P1>1. The P3 argument must be 1 (BTREE_INTKEY) for a rowid table it must be 2 (BTREE_BLOBKEY) for an index or WITHOUT ROWID table. The root page number of the new b-tree is stored in register P2.
    """
    CreateBtree = auto()
    """
    OpenWrite
    """
    OpenWrite = auto()
    """
    Set register P1 to have the value NULL as seen by the MakeRecord instruction, but do not free any string or blob memory associated with the register, so that if the value was a string or blob that was previously copied using SCopy, the copies will continue to be valid.
    """
    SoftNull = auto()
    """
    String: The string value P4 of length P1 (bytes) is stored in register P2.
    If P3 is not zero and the content of register P3 is equal to P5, then the datatype of the register P2 is converted to BLOB. The content is the same sequence of bytes, it is merely interpreted as a BLOB instead of a string, as if it had been CAST. In other words:

    if( P3!=0 and reg[P3]==P5 ) reg[P2] := CAST(reg[P2] as BLOB)
    """
    String = auto()

    """
    SCopy: Make a shallow copy of register P1 into register P2.
    
    This instruction makes a shallow copy of the value. If the value is a string or blob, then the copy is only a pointer to the original and hence if the original changes so will the copy. Worse, if the original is deallocated, the copy becomes invalid. Thus the program must guarantee that the original will not change during the lifetime of the copy. Use Copy to make a complete copy.
    """
    SCopy = auto()

    """
    Jump to P2 if the value in register P1 is NULL.
    """
    IsNull = auto()

    """
    Insert: Write an entry into the table of cursor P1. A new entry is created if it doesn't already exist or the data for an existing entry is overwritten. The data is the value MEM_Blob stored in register number P2. The key is stored in register P3. The key must be a MEM_Int.
    If the OPFLAG_NCHANGE flag of P5 is set, then the row change count is incremented (otherwise not). If the OPFLAG_LASTROWID flag of P5 is set, then rowid is stored for subsequent return by the sqlite3_last_insert_rowid() function (otherwise it is unmodified).

    If the OPFLAG_USESEEKRESULT flag of P5 is set, the implementation might run faster by avoiding an unnecessary seek on cursor P1. However, the OPFLAG_USESEEKRESULT flag must only be set if there have been no prior seeks on the cursor or if the most recent seek used a key equal to P3.

    If the OPFLAG_ISUPDATE flag is set, then this opcode is part of an UPDATE operation. Otherwise (if the flag is clear) then this opcode is part of an INSERT operation. The difference is only important to the update hook.

    Parameter P4 may point to a Table structure, or may be NULL. If it is not NULL, then the update-hook (sqlite3.xUpdateCallback) is invoked following a successful insert.

    (WARNING/TODO: If P1 is a pseudo-cursor and P2 is dynamically allocated, then ownership of P2 is transferred to the pseudo-cursor and register P2 becomes ephemeral. If the cursor is changed, the value of register P2 will then change. Make sure this does not cause any problems.)

    This instruction only works on tables. The equivalent instruction for indices is IdxInsert.
    """
    Insert = auto()
    """
    The 32-bit integer value P1 is written into register P2.
    """
    Integer = auto()
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
    NotNull: Jump to P2 if the value in register P1 is not NULL.
    """
    NotNull = auto()

    """
    SeekRowid: P1 is the index of a cursor open on an SQL table btree (with integer keys). 
    If register P3 does not contain an integer or if P1 does not contain a record with rowid P3 then jump immediately to P2. 

    Or, if P2 is 0, raise an SQLITE_CORRUPT error. 
    If P1 does contain a record with rowid P3 then leave the cursor pointing at that record and fall through to the next instruction.
    """
    SeekRowid = auto()
    """
    NewRowid: 

    Get a new integer record number (a.k.a "rowid") used as the key to a table. The record number is not previously used as a key in the database table that cursor P1 points to. The new record number is written written to register P2.
    If P3>0 then P3 is a register in the root frame of this VDBE that holds the largest previously generated record number. No new record numbers are allowed to be less than this value. When this value reaches its maximum, an SQLITE_FULL error is generated. The P3 register is updated with the ' generated record number. This P3 mechanism is used to help implement the AUTOINCREMENT feature.
    """
    NewRowid = auto()

    """
    Noop: Do nothing. This instruction is often useful as a jump destination.
    """
    Noop = auto()

    """
    NotExists:

    P1 is the index of a cursor open on an SQL table btree (with integer keys). P3 is an integer rowid. If P1 does not contain a record with rowid P3 then jump immediately to P2. Or, if P2 is 0, raise an SQLITE_CORRUPT error. If P1 does contain a record with rowid P3 then leave the cursor pointing at that record and fall through to the next instruction.
    The SeekRowid opcode performs the same operation but also allows the P3 register to contain a non-integer value, in which case the jump is always taken. This opcode requires that P3 always contain an integer.

    The NotFound opcode performs the same operation on index btrees (with arbitrary multi-value keys).

    This opcode leaves the cursor in a state where it cannot be advanced in either direction. In other words, the Next and Prev opcodes will not work following this opcode.
    """
    NotExists = auto()
    """
    MakeRecord:

    Convert P2 registers beginning with P1 into the record format use as a data record in a database table or as a key in an index. The Column opcode can decode the record later.
    P4 may be a string that is P2 characters long. The N-th character of the string indicates the column affinity that should be used for the N-th field of the index key.

    The mapping from character to affinity is given by the SQLITE_AFF_ macros defined in sqliteInt.h.

    If P4 is NULL then all index fields have the affinity BLOB.

    The meaning of P5 depends on whether or not the SQLITE_ENABLE_NULL_TRIM compile-time option is enabled:

    * If SQLITE_ENABLE_NULL_TRIM is enabled, then the P5 is the index of the right-most table that can be null-trimmed.

    * If SQLITE_ENABLE_NULL_TRIM is omitted, then P5 has the value OPFLAG_NOCHNG_MAGIC if the MakeRecord opcode is allowed to accept no-change records with serial_type 10. This value is only used inside an assert() and does not affect the end result.
    """
    MakeRecord = auto()
    """
    MustBeInt: Force the value in register P1 to be an integer. If the value in P1 is not an integer and cannot be converted into an integer without data loss, then jump immediately to P2, or if P2==0 raise an SQLITE_MISMATCH exception.
    """
    MustBeInt = auto()
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

    # TODO: we use Any type here.
    # because we store refs which resolve to address ints
    opcode: Opcode
    p1: Any = 0
    p2: Any = 0
    p3: int = 0
    p4: Optional[Any] = None  # TODO narrow type
    p5: int = 0


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

# Fancy counter
@dataclass
class Memory:
    address = 0

    def next_addr(self):
        self.address += 1
        return self.address - 1


class Compiler:
    """
    Given a Statement the compiler will produce a Program for the VM to execute.

    It's a vm itself to look up the internal schema table.
    """

    def __init__(self, pager: Pager, vm):
        self.pager = pager
        # These are needed to parse schema_table.sql_text
        # values to interpret column names and types
        self.lexer = StatementLexer()
        self.parser = Parser()
        # VM is used for internal programs
        # Like reading + writing to the schema table
        self.vm = vm
        self.init_schema_table()

    def init_schema_table(self):
        if len(self.pager) > 0:
            return

        program = self.compile(SCHEMA_TABLE_SQL_TEXT)
        [row for row in self.vm.execute(program)]

    def get_schema(self) -> List[List[Any]]:
        # Gets the current schema table values
        program = self.compile(f"SELECT * from {SCHEMA_TABLE_NAME}")
        values = [v for v in self.vm.execute(program)]
        return values

    def prepare(self, sql_text: str):
        tokens = self.lexer.lex(sql_text)
        stmts = self.parser.parse(tokens)

        return stmts

    def get_column_names_from_sql_text(self, sql_text: str):
        [statement] = self.prepare(sql_text)

        names = []
        if isinstance(statement, SelectStatement):
            for col in statement.items:
                if col.value != "*":
                    names.append(col.value)

        if isinstance(statement, CreateStatement):
            for col in statement.columns:
                if col.name.value != "*":
                    names.append(col.name.value)

        if isinstance(statement, InsertStatement):
            raise NotImplemented(
                "InsertStatement get_column_names_from_sql_text not NotImplemented"
            )

        return names

    def get_table_column_names(self, table_name):
        if table_name == SCHEMA_TABLE_NAME:
            return self.get_column_names_from_sql_text(SCHEMA_TABLE_SQL_TEXT)

        for values in self.get_schema():
            if values[1] == table_name:
                sql_text = values[2]
                return self.get_column_names_from_sql_text(sql_text)

        raise TableFoundException(f"Table: {table_name} not found")

    def get_column_indexes(self, statement: SelectStatement):
        column_index = []
        column_names = self.get_table_column_names(str(statement._from.value))

        for column_name in statement.items:
            if column_name.value == "*":
                # TODO need to handle this better.
                column_index = list(range(0, len(column_names)))
                break

            column_index.append(column_names.index(column_name.value))

        return column_index

    def get_table_root_page_number(self, table_name: str) -> int:
        # We don't need the schema here.
        if table_name == SCHEMA_TABLE_NAME:
            return 0

        for record in self.get_schema():
            if record[1] == table_name:
                root_page_number = record[3]
                return root_page_number

        raise TableFoundException(f"Table: {table_name} not found")

    def compile(self, sql_text) -> Program:
        # Initally we assume only one statement.
        [statement] = self.prepare(sql_text)
        program = Program([])
        memory = Memory()

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

            columns = []
            for i in self.get_column_indexes(statement):
                if i == 0:
                    # We assume for now columns at index 0
                    # are the row_id
                    columns.append(
                        Instruction(Opcode.Rowid, p1=0, p2=memory.next_addr())
                    )
                else:
                    columns.append(
                        Instruction(Opcode.Column, p1=0, p2=i, p3=memory.next_addr())
                    )

            program.instructions = [
                init,
                open_read,
                rewind,
                *columns,
                Instruction(
                    # TODO this assumes columns[0] is always a RowId
                    Opcode.ResultRow,
                    p1=columns[0].p2,
                    p2=columns[-1].p3,
                    p3=0,
                ),
                Instruction(Opcode.Next, p1=0, p2=3, p3=0, p5=1),
                Instruction(Opcode.Halt, p1=0, p2=0, p3=0),
                transaction,
                goto,
            ]

        if isinstance(statement, InsertStatement):
            transaction = Instruction(Opcode.Transaction, p1=0, p2=0, p3=21)
            init = Instruction(Opcode.Init, p2=transaction)
            table_page_number = self.get_table_root_page_number(
                str(statement.into.value)
            )

            # TODO: Don't understand softnull opcode.
            softnull = Instruction(Opcode.SoftNull, p1=memory.next_addr())
            open_write = Instruction(
                Opcode.OpenWrite, p1=0, p2=table_page_number, p3=0, p4=2
            )

            goto = Instruction(Opcode.Goto, p1=0, p2=open_write, p3=0)

            values = []

            affinities = ""
            # Taken from https://github.com/sqlcipher/sqlcipher/blob/master/src/sqliteInt.h#L2173-L2178
            ###
            # define SQLITE_AFF_NONE     0x40  /* '@' */
            # define SQLITE_AFF_BLOB     0x41  /* 'A' */
            # define SQLITE_AFF_TEXT     0x42  /* 'B' */
            # define SQLITE_AFF_NUMERIC  0x43  /* 'C' */
            # define SQLITE_AFF_INTEGER  0x44  /* 'D' */
            # define SQLITE_AFF_REAL     0x45  /* 'E' */
            ###
            for token in statement.values:
                if token.kind == Kind.integer:
                    affinities += "D"
                    values.append(
                        Instruction(
                            Opcode.Integer, p1=int(token.value), p2=memory.next_addr()
                        )
                    )

                if token.kind == Kind.text:
                    affinities += "B"
                    values.append(
                        Instruction(
                            Opcode.String,
                            p1=len(str(token.value)),
                            p2=memory.next_addr(),
                            p4=str(token.value),
                        )
                    )

            values.reverse()
            must_be_int = Instruction(Opcode.MustBeInt, p1=values[-1].p2)
            make_record = Instruction(
                Opcode.MakeRecord,
                p1=values[-1].p2,
                p2=len(affinities),
                p3=memory.next_addr(),
                p4=affinities,
            )

            program.instructions = [
                init,
                open_write,
                softnull,
                *values,
                # if row_id is null create a new one.
                Instruction(Opcode.NotNull, p1=values[-1].p2, p2=must_be_int),
                Instruction(Opcode.NewRowid, p1=0, p2=values[-1].p2),
                must_be_int,
                Instruction(Opcode.Noop),
                # P3 is an integer rowid. If P1 does not contain a record with rowid P3 then jump immediately to P2. Or, if P2 is 0, raise an SQLITE_CORRUPT error. If P1 does contain a record with rowid P3 then leave the cursor pointing at that record and fall through to the next instruction.
                # The SeekRowid opcode performs the same operation but also allows the P3 register to contain a non-integer value, in which case the jump is always taken. This opcode requires that P3 always contain an integer.
                Instruction(Opcode.NotExists, p1=0, p2=make_record, p3=values[-1].p2),
                # 1555 is the error code for key conflict
                Instruction(
                    Opcode.Halt, p1=1555, p2=2, p4=f"{statement.into.value}.id"
                ),
                make_record,
                Instruction(
                    Opcode.Insert,
                    p2=make_record.p3,
                    p3=values[-1].p2,
                    p4=statement.into.value,
                ),
                Instruction(Opcode.Halt),
                transaction,
                goto,
            ]

        if isinstance(statement, CreateStatement):
            transaction = Instruction(Opcode.Transaction, p1=0, p2=0, p3=21)
            goto = Instruction(Opcode.Goto, p1=0, p2=1, p3=0)

            init = Instruction(Opcode.Init, p2=transaction)

            create_btree = Instruction(
                Opcode.CreateBtree, p1=0, p2=memory.next_addr(), p3=1
            )

            new_row_id = Instruction(Opcode.NewRowid, p1=0, p2=memory.next_addr())
            row_id = Instruction(Opcode.Rowid, p1=0, p2=memory.next_addr())
            halt = Instruction(Opcode.Halt)
            is_null = Instruction(Opcode.IsNull, p1=row_id.p2, p2=halt)
            seek_row_id = Instruction(
                Opcode.SeekRowid, p1=0, p2=is_null, p3=new_row_id.p2
            )
            # affinities are always the same for schema table.
            # eg row_id, table_name, sql_text, root_page_number
            affinities = "DBBD"

            table_name_col = Instruction(
                Opcode.String,
                p1=len(str(statement.table.value)),
                p2=memory.next_addr(),
                p3=0,
                p4=str(statement.table.value),
            )  # Store "org" in addr p2

            sql_text_col = Instruction(
                Opcode.String,
                p1=len(sql_text),
                p2=memory.next_addr(),
                p3=0,
                p4=sql_text,
            )  # store sql_text addr p2

            root_page_number_col = Instruction(
                Opcode.SCopy, p1=create_btree.p2, p2=memory.next_addr()
            )  # This is to get root_page_number close in adress space to following values

            make_record = Instruction(
                Opcode.MakeRecord,
                p1=row_id.p2,
                p2=len(affinities),
                p3=memory.next_addr(),
                p4=affinities,
            )

            program.instructions = [
                init,
                create_btree,  # Save new btree root to reg 2
                Instruction(
                    Opcode.OpenWrite, p1=0, p2=0, p3=0, p4=2
                ),  # open write on schema table (root_page_number: 0)
                new_row_id,  # get new row_id for table cursor in p1 store it in addr p2
                seek_row_id,  # Seek cursor
                row_id,  # Store in register P2 an integer which is the key of the table entry that P1 is currently point to.
                is_null,  # If p1 addr is null jump to 11
                table_name_col,
                sql_text_col,
                root_page_number_col,
                make_record,
                Instruction(
                    Opcode.Insert, p2=make_record.p3, p3=1, p4=SCHEMA_TABLE_NAME
                ),
                halt,
                transaction,
                goto,
            ]

        program.resolve_refs()

        return program
