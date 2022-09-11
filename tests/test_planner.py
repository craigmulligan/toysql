from unittest import TestCase


class TestPlanner(TestCase):
    def test_select(self):
        """
        sqlite> explain select name from Artist limit 10;
        addr  opcode         p1    p2    p3    p4             p5  comment
        ----  -------------  ----  ----  ----  -------------  --  -------------
        0     Init           0     9     0                    0   Start at 9
        1     Integer        10    1     0                    0   r[1]=10; LIMIT counter
        2     OpenRead       0     3     0     2              0   root=3 iDb=0; Artist
        3     Rewind         0     8     0                    0
        4       Column         0     1     2                    0   r[2]=Artist.Name
        5       ResultRow      2     1     0                    0   output=r[2]
        6       DecrJumpZero   1     8     0                    0   if (--r[1])==0 goto 8
        7     Next           0     4     0                    1
        8     Halt           0     0     0                    0
        9     Transaction    0     0     21    0              1   usesStmtJournal=0
        10    Goto           0     1     0                    0

        Given statements we should create a VM instructions to execute the query.
        """

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

    def insert(self):
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
