from snapshottest import TestCase
import random
from toysql.btree import BTree
from toysql.pager import Pager
from toysql.record import Record, DataType
from toysql.table import Table
import tempfile


class TestBTree(TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_file_path = self.temp_dir.name + "/__testdb__.db"
        self.pager = Pager(self.db_file_path)

        def create_record(row_id: int, text: str):
            return Record([[DataType.INTEGER, row_id], [DataType.TEXT, text]])

        self.create_record = create_record

        return super().setUp()

    def cleanUp(self) -> None:
        self.temp_dir.cleanup()

    def test_insert_select(self):
        btree = BTree(self.pager, self.pager.new())
        table = Table("users", btree)

        records = []

        for n in [5, 15, 25, 35, 45]:
            record = self.create_record(n, f"hello-{n}")
            table.insert(record)
            records.append(record)

        for i, record in enumerate(table.select()):
            assert record == records[i]
