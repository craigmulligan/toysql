from toysql.compiler import Compiler, Instruction, Opcode, SCHEMA_TABLE_NAME
from tests.fixtures import Fixtures
from unittest.mock import Mock

# http://chi.cs.uchicago.edu/chidb/architecture.html#chidb-dbm

class TestCompiler(Fixtures):
    def setUp(self) -> None:
        super().setUp()
        sql_text = "CREATE TABLE user (id INT, name text(12), email text(255));"
        self.root_page_number = 3
        self.vm = Mock()
        self.vm.execute = Mock(return_value=[])
        self.compiler = Compiler(self.pager, self.vm)
        self.compiler.get_schema = Mock(
            return_value=[[1, "user", sql_text, self.root_page_number]]
        )

    def tearDown(self) -> None:

        return super().tearDown()

    def test_schema_select(self):
        program = self.compiler.compile(f"select * from {SCHEMA_TABLE_NAME};")
        assert program.instructions == [
            Instruction(Opcode.Init, p2=10),
            Instruction(Opcode.OpenRead, p1=0, p2=0, p3=0, p4=2),
            Instruction(Opcode.Rewind, p1=0, p2=7, p3=0),
            Instruction(Opcode.Rowid, p1=0, p2=0),
            Instruction(Opcode.Column, p1=0, p2=1, p3=1),
            Instruction(Opcode.Column, p1=0, p2=2, p3=2),
            Instruction(Opcode.Column, p1=0, p2=3, p3=3),
            Instruction(Opcode.ResultRow, p1=0, p2=3, p3=0),
            Instruction(Opcode.Next, p1=0, p2=3, p3=0, p5=1),
            Instruction(Opcode.Halt, p1=0, p2=0, p3=0),
            Instruction(Opcode.Transaction, p1=0, p2=0, p3=21),
            Instruction(Opcode.Goto, p1=0, p2=1, p3=0),
        ]

    def test_select(self):
        """
        select * from artist;

        addr  opcode         p1    p2    p3    p4             p5  comment
        ----  -------------  ----  ----  ----  -------------  --  -------------
        0     Init           0     8     0                    0   Start at 8
        1     OpenRead       0     3     0     2              0   root=3 iDb=0; artist
        2     Rewind         0     7     0                    0
        3       Rowid          0     1     0                    0   r[1]=user.rowid
        4       Column         0     1     1                    0   r[2]=user.name
        4       Column         0     2     2                    0   r[3]=user.email
        5       ResultRow      1     2     0                    0   output=r[1..2]
        6     Next           0     3     0                    1
        7     Halt           0     0     0                    0
        8     Transaction    0     0     21    0              1   usesStmtJournal=0
        9     Goto           0     1     0                    0

        Given statements we should create a VM instructions to execute the query.
        """
        program = self.compiler.compile("select * from user;")

        assert program.instructions == [
            Instruction(Opcode.Init, p2=9),
            Instruction(Opcode.OpenRead, p1=0, p2=self.root_page_number, p3=0, p4=2),
            Instruction(Opcode.Rewind, p1=0, p2=7),
            Instruction(Opcode.Rowid, p1=0, p2=0),
            Instruction(Opcode.Column, p1=0, p2=1, p3=1),
            Instruction(Opcode.Column, p1=0, p2=2, p3=2),
            Instruction(Opcode.ResultRow, p1=0, p2=2),
            Instruction(Opcode.Next, p1=0, p2=3, p3=0, p5=1),
            Instruction(Opcode.Halt, p1=0, p2=0),
            Instruction(Opcode.Transaction, p1=0, p2=0, p3=21),
            Instruction(Opcode.Goto, p1=0, p2=1),
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

        # We are going a fair ways offscript here.
        # Ignoring instructions 1-13 as they are schema cookie replated.
        # It's simpler to imagine a schema change is a create statement is just an insert on the schema table.
        program = self.compiler.compile('create table "org" (id INT, name TEXT);')

        # TODO CreateBtree will write to disk. We need to check that we can recover
        # with Transactions

        assert program.instructions == [
            Instruction(Opcode.Integer, p1=0, p2=0),
            Instruction(
                Opcode.OpenWrite, p1=0, p2=0, p3=5
            ),  # open write on schema table (root_page_number: 0)
            Instruction(
                Opcode.CreateTable, p1=4
            ),  # Save new btree root to reg 2
            Instruction(
                Opcode.String, p1=5, p2=1, p4="table"
            ), 
            Instruction(
                Opcode.String, p1=8, p2=2, p4="products"
            ),
            Instruction(
                Opcode.String, p1=8, p2=3, p4="products"
            ),
            Instruction(
                Opcode.String, p1=73, p2=5, p4="CREATE TABLE products(code INTEGER PRIMARY KEY, name TEXT, price INTEGER)"
            ),
            Instruction(
                Opcode.MakeRecord, p1=1, p2=5, p3=6
            ),
            Instruction(Opcode.Integer, p1=1, p2=7),
            Instruction(Opcode.Insert, p1=0, p2=6, p3=7),
            Instruction(Opcode.Halt),
            Instruction(Opcode.Close, p1=0)
        ]

    def test_insert(self):
        """
        sqlite> explain insert into Artist values (9999, "Craigo");
        addr  opcode         p1    p2    p3    p4             p5  comment
        ----  -------------  ----  ----  ----  -------------  --  -------------
        0     Init           0     14    0                    0   Start at 14
        1     OpenWrite      0     3     0     2              0   root=3 iDb=0; Artist
        2     SoftNull       2     0     0                    0   r[2]=NULL
        3     String8        0     3     0     Craigo         0   r[3]='Craigo'
        4     Integer        9999  1     0                    0   r[1]=9999
        5     NotNull        1     7     0                    0   if r[1]!=NULL goto 7
        6     NewRowid       0     1     0                    0   r[1]=rowid
        7     MustBeInt      1     0     0                    0
        8     Noop           0     0     0                    0   uniqueness check for ROWID
        9     NotExists      0     11    1                    0   intkey=r[1]
        10    Halt           1555  2     0     Artist.ArtistId  2
        11    MakeRecord     2     2     4     DB             0   r[4]=mkrec(r[2..3])
        12    Insert         0     4     1     Artist         49  intkey=r[1] data=r[4]
        13    Halt           0     0     0                    0
        14    Transaction    0     1     21    0              1   usesStmtJournal=0
        15    Goto           0     1     0                    0
        """
        program = self.compiler.compile(
            "insert into user values (9999, 'craig', 'craig@example.com');"
        )

        assert program.instructions == [
            Instruction(Opcode.Init, p2=15),
            Instruction(Opcode.OpenWrite, p1=0, p2=self.root_page_number, p3=0, p4=2),
            Instruction(Opcode.SoftNull),
            Instruction(Opcode.String, p1=17, p2=3, p3=0, p4="craig@example.com"),
            Instruction(Opcode.String, p1=5, p2=2, p3=0, p4="craig"),
            Instruction(Opcode.Integer, p1=9999, p2=1, p3=0),
            Instruction(Opcode.NotNull, p1=1, p2=8),
            Instruction(Opcode.NewRowid, p1=0, p2=1),
            Instruction(Opcode.MustBeInt, p1=1),
            Instruction(Opcode.Noop),
            Instruction(Opcode.NotExists, p1=0, p2=12, p3=1),
            Instruction(Opcode.Halt, p1=1555, p2=2, p4="user.row_id"),
            Instruction(Opcode.MakeRecord, p1=1, p2=3, p3=4, p4="DBB"),
            Instruction(Opcode.Insert, p2=4, p3=1, p4="user"),
            Instruction(Opcode.Halt),
            Instruction(Opcode.Transaction, p1=0, p2=0, p3=21),
            Instruction(Opcode.Goto, p1=0, p2=1, p3=0),
        ]
