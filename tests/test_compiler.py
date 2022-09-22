from toysql.compiler import Compiler, Instruction, Opcode, SCHEMA_TABLE_NAME
from tests.fixtures import Fixtures
from unittest.mock import Mock


class TestCompiler(Fixtures):
    def setUp(self) -> None:
        super().setUp()
        sql_text = "CREATE TABLE user (id INT, name text(12), email text(255));"
        self.root_page_number = 3
        self.vm = Mock()
        self.compiler = Compiler(self.pager, self.vm)
        self.compiler.get_schema = Mock(
            return_value=[[1, "user", sql_text, self.root_page_number]]
        )

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
        sqlite> explain create table "people" (id INT, name TEXT, email Text);
        addr  opcode         p1    p2    p3    p4             p5  comment
        ----  -------------  ----  ----  ----  -------------  --  -------------
        0     Init           0     28    0                    0   Start at 28
        1     ReadCookie     0     3     2                    0
        2     If             3     5     0                    0
        3     SetCookie      0     2     4                    0
        4     SetCookie      0     5     1                    0
        5     CreateBtree    0     2     1                    0   r[2]=root iDb=0 flags=1
        6     OpenWrite      0     1     0     5              0   root=1 iDb=0
        7     NewRowid       0     1     0                    0   r[1]=rowid
        8     Blob           6     3     0                   0   r[3]= (len=6)
        9     Insert         0     3     1                    8   intkey=r[1] data=r[3]
        10    Close          0     0     0                    0
        11    Close          0     0     0                    0
        12    Null           0     4     5                    0   r[4..5]=NULL
        13    Noop           2     0     4                    0
        14    OpenWrite      1     1     0     5              0   root=1 iDb=0; sqlite_master
        15    SeekRowid      1     17    1                    0   intkey=r[1]
        16    Rowid          1     5     0                    0   r[5]= rowid of 1
        17    IsNull         5     25    0                    0   if r[5]==NULL goto 25
        18    String8        0     6     0     table          0   r[6]='table'
        19    String8        0     7     0     people         0   r[7]='people'
        20    String8        0     8     0     people         0   r[8]='people'
        21    SCopy          2     9     0                    0   r[9]=r[2]
        22    String8        0     10    0     CREATE TABLE "people" (id INT, name TEXT, email Text)  0   r[10]='CREATE TABLE "people" (id INT, name TEXT, email Text)'
        23    MakeRecord     6     5     4     BBBDB          0   r[4]=mkrec(r[6..10])
        24    Insert         1     4     5                    0   intkey=r[5] data=r[4]
        25    SetCookie      0     1     2                    0
        26    ParseSchema    0     0     0     tbl_name='people' AND type!='trigger'  0
        27    Halt           0     0     0                    0
        28    Transaction    0     1     1     0              1   usesStmtJournal=1
        29    Goto           0     1     0                    0
        """

        # We are going a fair ways offscript here.
        # Ignoring instructions 1-13 as they are schema cookie replated.
        # It's simpler to imagine a schema change is a create statement is just an insert on the schema table.
        program = self.compiler.compile('create table "org" (id INT, name TEXT);')

        # TODO CreateBtree will write to disk. We need to check that we can recover
        # with Transactions

        assert program.instructions == [
            Instruction(Opcode.Init, p2=13),
            Instruction(
                Opcode.CreateBtree, p1=0, p2=0, p3=1
            ),  # Save new btree root to reg 2
            Instruction(
                Opcode.OpenWrite, p1=0, p2=0, p3=0, p4=2
            ),  # open write on schema table (root_page_number: 0)
            Instruction(
                Opcode.NewRowid, p1=0, p2=1
            ),  # get new row_id for table cursor in p1 store it in addr p2
            Instruction(Opcode.SeekRowid, p1=0, p2=6, p3=1),
            Instruction(
                Opcode.Rowid, p1=0, p2=2
            ),  # Store in register P2 an integer which is the key of the table entry that P1 is currently point to.
            Instruction(Opcode.IsNull, p1=2, p2=12),  # If p1 addr is null jump to p2
            Instruction(
                Opcode.String, p1=3, p2=3, p3=0, p4="org"
            ),  # Store "org" in addr p2
            Instruction(
                Opcode.String,
                p1=39,
                p2=4,
                p3=0,
                p4='create table "org" (id INT, name TEXT);',
            ),  # store sql_text addr p2
            Instruction(
                Opcode.SCopy, p1=0, p2=5
            ),  # This is to get root_page_number close in adress space to following values
            Instruction(
                Opcode.MakeRecord, p1=2, p2=4, p3=6, p4="DBBD"
            ),  # create record
            Instruction(Opcode.Insert, p2=6, p3=1, p4=SCHEMA_TABLE_NAME),
            Instruction(Opcode.Halt),
            Instruction(Opcode.Transaction, p1=0, p2=0, p3=21),
            Instruction(Opcode.Goto, p1=0, p2=1, p3=0),
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
            Instruction(Opcode.Halt, p1=1555, p2=2, p4="user.id"),
            Instruction(Opcode.MakeRecord, p1=1, p2=3, p3=4, p4="DBB"),
            Instruction(Opcode.Insert, p2=4, p3=1, p4="user"),
            Instruction(Opcode.Halt),
            Instruction(Opcode.Transaction, p1=0, p2=0, p3=21),
            Instruction(Opcode.Goto, p1=0, p2=1, p3=0),
        ]
