import random

from toysql.vm import VM, SCHEME_TABLE_NAME
from toysql.exceptions import DuplicateKeyException
from tests.fixtures import Fixtures


class TestVM(Fixtures):
    def setUp(self) -> None:
        super().setUp()

        self.vm = VM(self.db_file_path)
        self.table_name = "users"

        self.vm.execute(
            f"CREATE TABLE {self.table_name} (id INT, name TEXT(32), email TEXT(255));"
        )

        return

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

    def test_insert_and_select_with_named_columns(self):
        """
        Given a list of columns a select statement
        returns a subset of columns.
        """
        rows = [
            [1, "fred", "fred@flintstone.com"],
            [2, "pebbles", "pebbles@flintstone.com"],
        ]

        for row in rows:
            self.vm.execute(
                f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
            )

        [records] = self.vm.execute(f"SELECT name, email FROM {self.table_name}")

        assert len(records) == len(rows)

        for i, record in enumerate(records):
            _, name, email = record.values
            _, input_name, input_email = rows[i]
            assert input_name == name
            assert input_email == email

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
