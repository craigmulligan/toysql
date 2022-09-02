from toysql.parser import InsertStatement, SelectStatement
from toysql.vm import VM, SCHEME_TABLE_NAME
from toysql.exceptions import DuplicateKeyException
from toysql.pager import Pager

from tests.fixtures import Fixtures
from unittest import TestCase

import random
import tempfile


class TestVM(TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_file_path = self.temp_dir.name + "/__testdb__.db"
        self.pager = Pager(self.db_file_path)

        def create_record(row_id: int, text: str):
            return Record(
                [
                    [DataType.INTEGER, row_id],
                    [DataType.TEXT, text],
                    [DataType.TEXT, text],
                ]
            )

        self.create_record = create_record

        self.vm = VM(self.db_file_path)
        self.table_name = "users"

        self.vm.execute(
            f"CREATE TABLE {self.table_name} (id INT, name TEXT(32), email TEXT(255));"
        )

        return super().setUp()

    def cleanUp(self) -> None:
        self.temp_dir.cleanup()

    def test_system_schema_table(self):
        """
        When we init the VM we should auto
        create a schema table.
        """

        assert self.vm.tables[SCHEME_TABLE_NAME]
        assert len(self.vm.tables.keys()) == 2
        [records] = self.vm.execute(f"SELECT * FROM {SCHEME_TABLE_NAME}")
        assert len(records) == 1
        assert records[0].values[1][1] == self.table_name

    def test_insert_and_select(self):
        rows = [
            [1, "fred", "fred@flintstone.com"],
            [2, "pebbles", "pebbles@flintstone.com"],
        ]

        for row in rows:
            self.vm.execute(
                f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
            )

        [records] = self.vm.execute(f"SELECT * FROM {self.table_name}")

        assert len(records) == len(rows)
        for i, record in enumerate(records):
            assert record.row_id == rows[i][0]

    def test_vm_duplicate_key(self):
        row = (1, "fred", "fred@flintstone.com")
        row_2 = (1, "pebbles", "pebbles@flintstone.com")
        self.vm.execute(
            f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
        )

        with self.assertRaises(DuplicateKeyException):
            self.vm.execute(
                f"INSERT INTO {self.table_name} VALUES ({row_2[0]}, '{row_2[1]}', '{row_2[2]}');"
            )

    def test_multipage_insert_select(self):
        keys = [n for n in range(10)]
        random.shuffle(keys)

        for n in keys:
            row = [n, f"fred-{n}", f"fred-{n}@flintstone.com"]
            self.vm.execute(
                f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
            )

        [records] = self.vm.execute(f"SELECT * FROM {self.table_name}")
        keys.sort()

        assert len(records) == len(keys)
        assert [record.row_id for record in records] == keys
