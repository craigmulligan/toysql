from toysql.compiler import Compiler, Instruction, Opcode, SCHEMA_TABLE_NAME
from tests.fixtures import Fixtures
from unittest.mock import Mock, patch

# http://chi.cs.uchicago.edu/chidb/architecture.html#chidb-dbm


class TestCompiler(Fixtures):
    def setUp(self) -> None:
        super().setUp()
        self.sql_text = (
            "CREATE TABLE products(code INTEGER PRIMARY KEY, name TEXT, price INTEGER)"
        )
        self.root_page_number = 2
        self.compiler = Compiler(self.pager)
        self.compiler.get_schema = Mock(
            return_value=[
                [
                    1,
                    "table",
                    "products",
                    "products",
                    self.root_page_number,
                    self.sql_text,
                ]
            ]
        )

    def tearDown(self) -> None:
        return super().tearDown()

    def test_schema_select(self):
        program = self.compiler.compile(f"select * from {SCHEMA_TABLE_NAME};")
        assert program.instructions == [
            Instruction(Opcode.Integer, p1=0, p2=0),
            Instruction(Opcode.OpenRead, p1=0, p2=0, p3=4),
            Instruction(Opcode.Rewind, p1=0, p2=11),
            Instruction(Opcode.Key, p1=0, p2=1),
            Instruction(Opcode.Column, p1=0, p2=1, p3=2),
            Instruction(Opcode.Column, p1=0, p2=2, p3=3),
            Instruction(Opcode.Column, p1=0, p2=3, p3=4),
            Instruction(Opcode.Column, p1=0, p2=4, p3=5),
            Instruction(Opcode.Column, p1=0, p2=5, p3=6),
            Instruction(Opcode.ResultRow, p1=1, p2=6),
            Instruction(Opcode.Next, p1=0, p2=3),
            Instruction(Opcode.Close, p1=0),
            Instruction(Opcode.Halt, p1=0, p2=0),
        ]

    def test_select(self):
        """
        # Open the courses table using cursor 0
        Integer      2  0  _  _
        OpenRead     0  0  4  _

        # Go to the first entry. If the database is empty,
        # jump to the end of the program
        Rewind       0  9  _  _

        # Fetch the key of the row, plus the values
        # of "name", "prof", and "dept"
        Key          0  1  _  _
        Column       0  1  2  _
        Column       0  2  3  _
        Column       0  3  4  _
        ResultRow    1  4  _  _
        Next         0  3  _  _

        # Close the cursor
        Close        0  _  _  _
        Halt         _  _  _  _
        """
        program = self.compiler.compile("select * from products;")

        assert program.instructions == [
            Instruction(Opcode.Integer, p1=2, p2=0),
            Instruction(Opcode.OpenRead, p1=0, p2=0, p3=4),
            Instruction(Opcode.Rewind, p1=0, p2=8),
            Instruction(Opcode.Key, p1=0, p2=1),
            Instruction(Opcode.Column, p1=0, p2=1, p3=2),
            Instruction(Opcode.Column, p1=0, p2=2, p3=3),
            Instruction(Opcode.ResultRow, p1=1, p2=3),
            Instruction(Opcode.Next, p1=0, p2=3),
            Instruction(Opcode.Close, p1=0),
            Instruction(Opcode.Halt, p1=0, p2=0),
        ]

    def test_create(self):
        """
        # Open the schema table using cursor 0
        Integer      1  0  _  _
        OpenWrite    0  0  5  _

        # Create a new B-Tree, store its root page in register 4
        CreateTable  4  _  _  _

        # Create the rest of the record
        String       5  1  _  "table"
        String       8  2  _  "products"
        String       8  3  _  "products"
        String       73 5  _  "CREATE TABLE products(code INTEGER PRIMARY KEY, name TEXT, price INTEGER)"

        MakeRecord   1  5  6  _
        Integer      1  7  _  _

        # Insert the new record
        Insert       0  6  7  _

        # Close the cursor
        Close        0  _  _  _
        """

        sql_text = self.sql_text

        with patch.object(self.compiler, "get_schema", return_value=[]):
            program = self.compiler.compile(sql_text)

        assert program.instructions == [
            Instruction(Opcode.Integer, p1=0, p2=0),
            Instruction(Opcode.OpenWrite, p1=0, p2=0, p3=5),
            Instruction(Opcode.CreateTable, p1=4),
            Instruction(Opcode.String, p1=5, p2=1, p4="table"),
            Instruction(Opcode.String, p1=8, p2=2, p4="products"),
            Instruction(Opcode.String, p1=8, p2=3, p4="products"),
            Instruction(Opcode.String, p1=len(sql_text), p2=5, p4=sql_text),
            Instruction(Opcode.MakeRecord, p1=1, p2=5, p3=6),
            Instruction(Opcode.Integer, p1=1, p2=7),
            Instruction(Opcode.Insert, p1=0, p2=6, p3=7),
            Instruction(Opcode.Close, p1=0),
        ]

    def test_insert_with_primary_key(self):
        """
        Here we declare the primary key at the end
        and make sure it's still used for the Key instruction.
        """
        sql_text = (
            "CREATE TABLE products(name TEXT, price INTEGER, code INTEGER PRIMARY KEY)"
        )
        schema = [[1, "table", "products", "products", self.root_page_number, sql_text]]

        stmt = """INSERT INTO products VALUES('Hard Drive', 240, 1)"""
        with patch.object(self.compiler, "get_schema", return_value=schema):
            program = self.compiler.compile(stmt)

        assert program.instructions == [
            Instruction(Opcode.Integer, p1=self.root_page_number, p2=0, p3=0),
            Instruction(Opcode.OpenWrite, p1=0, p2=0, p3=3),
            Instruction(Opcode.String, p1=10, p2=1, p4="Hard Drive"),
            # Instruction(Opcode.Null, p2=2), TODO: Not sure why Null is necessary here?
            Instruction(Opcode.Integer, p1=240, p2=2),
            Instruction(Opcode.Integer, p1=1, p2=3),
            Instruction(Opcode.MakeRecord, p1=1, p2=3, p3=4),
            Instruction(Opcode.Insert, p1=0, p2=4, p3=3),
            Instruction(Opcode.Close, p1=0),
        ]

    def test_insert(self):
        """
        # Open the "products" table using cursor 0
        Integer      2  0  _  _
        OpenWrite    0  0  3  _

        # Create the record
        Integer      1    1  _  _
        Null         _    2  _  _
        String       10   3  _  "Hard Drive"
        Integer      240  4  _  _

        MakeRecord   2  3  5  _

        # Insert the new record
        Insert       0  5  1  _

        # Close the cursor
        Close        0  _  _  _

        R_0 integer 2
        R_1 integer 1
        R_2 null
        R_3 string "Hard Drive"
        R_4 integer 240
        R_5 binary
        """
        stmt = """INSERT INTO products VALUES(1, 'Hard Drive', 240)"""

        program = self.compiler.compile(stmt)

        assert program.instructions == [
            Instruction(Opcode.Integer, p1=self.root_page_number, p2=0, p3=0),
            Instruction(Opcode.OpenWrite, p1=0, p2=0, p3=3),
            Instruction(Opcode.Integer, p1=1, p2=1),
            # Instruction(Opcode.Null, p2=2), TODO: Not sure why Null is necessary here?
            Instruction(Opcode.String, p1=10, p2=2, p4="Hard Drive"),
            Instruction(Opcode.Integer, p1=240, p2=3),
            Instruction(Opcode.MakeRecord, p1=1, p2=3, p3=4),
            Instruction(Opcode.Insert, p1=0, p2=4, p3=1),
            Instruction(Opcode.Close, p1=0),
        ]
