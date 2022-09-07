from snapshottest import TestCase
from toysql.btree import BTree
from toysql.record import Record, DataType
from toysql.table import Table
from tests.fixtures import Fixtures


class TestBTree(Fixtures, TestCase):
    def setUp(self) -> None:
        super().setUp()

        def create_record(row_id: int, text: str):
            return Record([[DataType.INTEGER, row_id], [DataType.TEXT, text]])

        self.create_record = create_record

    def test_insert_select(self):
        page_number = self.pager.new()
        btree = BTree(self.pager, page_number)
        table = Table("users", btree)

        records = []

        for n in [5, 15, 25, 35, 45]:
            record = self.create_record(n, f"hello-{n}")
            table.insert(record)
            records.append(record)

        results = [r for r in table.select()]

        for i, record in enumerate(records):
            assert record == results[i]

        # Now load from disk and read again.
        new_btree = BTree(self.pager, page_number)
        new_table = Table("users", new_btree)

        results = [r for r in new_table.select()]
        for i, record in enumerate(records):
            assert record == results[i]