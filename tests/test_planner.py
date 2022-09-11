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
