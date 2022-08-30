# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot


snapshots = Snapshot()

snapshots['TestBTree::test_btree 1'] = '''25
\t15
\t\t\x1b[1;32m 5\x1b[0m
\t\t\x1b[1;32m 15\x1b[0m

\t35
\t\t\x1b[1;32m 25\x1b[0m
\t\t\x1b[1;32m 35, 45\x1b[0m


'''

snapshots['TestBTree::test_btree_x 1'] = '''15
\t\x1b[1;32m \x1b[0m
\t\x1b[1;32m \x1b[0m

'''
