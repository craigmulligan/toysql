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
from toysql.btree import BTree, Cursor


"""
All opcodes can be found here: https://www.sqlite.org/opcode.html
This is a subset of them implemented for toysqls feature set.
"""
# http://chi.cs.uchicago.edu/chidb/architecture.html#chidb-dbm


class Opcode(Enum):
    # Register Manipulation Instructions
    Integer = auto()
    String = auto()
    Null = auto()
    SCopy = auto()

    # Control Flow Instructions
    Eq = auto()
    Ne = auto()
    Lt = auto()
    Le = auto()
    Gt = auto()
    Ge = auto()
    Halt = auto()
    Noop = auto()

    # Database Opening and Closing Instructions
    OpenRead = auto()
    OpenWrite = auto()
    Close = auto()

    # Cursor Manipulation Instructions
    Rewind = auto()
    Next = auto()
    Prev = auto()
    Seek = auto()
    SeekGt = auto()
    SeekGe = auto()
    SeekLt = auto()
    IdxGt = auto()
    IdxLt = auto()
    IdxLe = auto()

    # Cursor Access Instructions
    Column = auto()
    Key = auto()
    IdxPKey = auto()

    # Database Record Instructions
    MakeRecord = auto()
    ResultRow = auto()

    # Insert instructions
    Insert = auto()
    IdxInsert = auto()

    # B-Tree Creation Instructions
    CreateTable = auto()
    CreateIndex = auto()


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
SCHEMA_TABLE_SQL_TEXT = f"CREATE TABLE {SCHEMA_TABLE_NAME} (id INT, schema_type TEXT, name TEXT, t_name TEXT, sql_text TEXT, root_page_number INT);"


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
    """

    def __init__(self, pager: Pager):
        self.pager = pager
        # These are needed to parse schema_table.sql_text
        # values to interpret column names and types
        self.lexer = StatementLexer()
        self.parser = Parser()
        self.init_schema_table()

    def init_schema_table(self):
        if len(self.pager) == 0:
            self.pager.new()

    def get_schema(self) -> List[List[Any]]:
        # Gets the current schema table values
        cursor = Cursor(BTree(self.pager, 0))
        rows = [[r[1] for r in record.values] for record in cursor]

        return rows

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
            if values[2] == table_name:
                sql_text = values[5]
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
            if record[2] == table_name:
                root_page_number = record[4]
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

            column_count = 4
            table_cursor = 0

            program.instructions.append(
                Instruction(Opcode.Integer, p1=table_page_number, p2=memory.next_addr())
            )
            program.instructions.append(
                Instruction(Opcode.OpenRead, p1=table_cursor, p2=0, p3=column_count)
            )

            close = Instruction(Opcode.Close, p1=0)
            program.instructions.append(Instruction(Opcode.Rewind, p1=0, p2=close))

            key_addr = memory.next_addr()
            program.instructions.append(Instruction(Opcode.Key, p1=0, p2=key_addr))

            columns = []
            column_indexes = self.get_column_indexes(statement)
            for i in column_indexes:
                if i > 0:
                    columns.append(
                        Instruction(Opcode.Column, p1=0, p2=i, p3=memory.next_addr())
                    )

            first_column_addr = key_addr if 0 in column_indexes else columns[0].p3
            program.instructions.extend(columns)
            program.instructions.append(
                Instruction(Opcode.ResultRow, p1=first_column_addr, p2=len(columns) + 1)
            )
            program.instructions.append(Instruction(Opcode.Next, p1=0, p2=3))
            program.instructions.extend([close, Instruction(Opcode.Halt, p1=0, p2=0)])

        if isinstance(statement, InsertStatement):
            table_cursor = 0
            table_page_number = self.get_table_root_page_number(
                str(statement.into.value)
            )
            table_page_number_addr = memory.next_addr()

            program.instructions.append(
                Instruction(
                    Opcode.Integer,
                    p1=table_page_number,
                    p2=table_page_number_addr,
                    p3=0,
                )
            )
            # TODO: get number of columns from schema stmt - replace 3.
            program.instructions.append(
                Instruction(
                    Opcode.OpenWrite, p1=table_cursor, p2=table_page_number_addr, p3=3
                )
            )

            first_column_addr = None
            for token in statement.values:
                addr = memory.next_addr()
                if first_column_addr is None:
                    first_column_addr = addr

                if token.kind == Kind.integer:
                    program.instructions.append(
                        Instruction(Opcode.Integer, p1=int(token.value), p2=addr)
                    )

                if token.kind == Kind.text:
                    program.instructions.append(
                        Instruction(
                            Opcode.String,
                            p1=len(str(token.value)),
                            p2=addr,
                            p4=token.value,
                        )
                    )

                # TODO: handle NULL.

            record_addr = memory.next_addr()
            program.instructions.append(
                Instruction(
                    Opcode.MakeRecord,
                    p1=first_column_addr,
                    p2=len(statement.values),
                    p3=record_addr,
                )
            )

            # How is this figured? This means we need to load the btree cursor?
            assert first_column_addr
            program.instructions.append(
                Instruction(
                    Opcode.Insert, p1=table_cursor, p2=record_addr, p3=first_column_addr
                )
            )

            program.instructions.append(
                Instruction(Opcode.Close, p1=table_cursor),
            )

        if isinstance(statement, CreateStatement):
            instructions = []
            schema_root_page_num = 0
            schema_root_page_num_addr = memory.next_addr()
            # Layout the registers
            schema_type_addr = memory.next_addr()
            schema_type = "table"
            item_name_addr = memory.next_addr()
            item_name = str(statement.table.value)
            associated_table_name_addr = memory.next_addr()
            associated_table_name = str(statement.table.value)
            root_page_num_addr = memory.next_addr()
            text = sql_text
            text_addr = memory.next_addr()

            column_count = 5

            schema_cursor = 0

            instructions.append(
                Instruction(
                    Opcode.Integer,
                    p1=schema_root_page_num,
                    p2=schema_root_page_num_addr,
                )
            )
            instructions.append(
                Instruction(
                    Opcode.OpenWrite,
                    p1=schema_cursor,
                    p2=schema_root_page_num_addr,
                    p3=column_count,
                )
            )
            instructions.append(Instruction(Opcode.CreateTable, p1=root_page_num_addr))
            instructions.append(
                Instruction(
                    Opcode.String,
                    p1=len(schema_type),
                    p2=schema_type_addr,
                    p4=schema_type,
                )
            )
            instructions.append(
                Instruction(
                    Opcode.String, p1=len(item_name), p2=item_name_addr, p4=item_name
                )
            )
            instructions.append(
                Instruction(
                    Opcode.String,
                    p1=len(associated_table_name),
                    p2=associated_table_name_addr,
                    p4=associated_table_name,
                )
            )

            instructions.append(
                Instruction(
                    Opcode.String,
                    p1=len(text),
                    p2=text_addr,
                    p4=text,
                )
            )

            record_addr = memory.next_addr()
            instructions.append(
                Instruction(
                    Opcode.MakeRecord,
                    p1=schema_type_addr,
                    p2=column_count,
                    p3=record_addr,
                )
            )

            primary_key = len(self.get_schema()) + 1
            primary_key_addr = memory.next_addr()
            # TODO: I'm not sure why we don't use seek end + Key opcodes to get the primary key?
            instructions.append(
                Instruction(Opcode.Integer, p1=primary_key, p2=primary_key_addr)
            )

            instructions.append(
                Instruction(
                    Opcode.Insert, p1=schema_cursor, p2=record_addr, p3=primary_key_addr
                ),
            )

            instructions.append(
                Instruction(Opcode.Close, p1=schema_cursor),
            )

            program.instructions = instructions

        program.resolve_refs()

        return program
