import unittest
from toysql.vm import VM
from tests.fixtures import Fixtures
from toysql.compiler import Compiler, SCHEMA_TABLE_NAME
import random


class TestVM(Fixtures):
    def setUp(self) -> None:
        super().setUp()
        self.table_name = "users"
        create_stmt = (
            f"CREATE TABLE {self.table_name} (id INT, name TEXT, email TEXT);"
        )

        self.vm = VM(self.pager)
        self.compiler = Compiler(self.pager)

        def execute(sql: str):
            program = self.compiler.compile(sql)
            return [row for row in self.vm.execute(program)]

        self.execute = execute

        program = self.compiler.compile(create_stmt)
        [row for row in self.vm.execute(program)]

        self.root_page_number = 1

    def test_create(self):
        """
        When we init the VM we should auto
        create a schema table.
        """
        table_name = "org"
        create_stmt = f"CREATE TABLE {table_name} (id INT, name TEXT);"
        self.execute(create_stmt)
        records = self.execute(f"SELECT * FROM {SCHEMA_TABLE_NAME}")

        assert len(records) == 2
        new_row = records[1]
        # TODO: Listing key here twice.
        assert new_row == [2, 2, "table", table_name, table_name, 2,  create_stmt]

    def test_insert_and_select_x(self):
        rows = [
            [1, "fred", "fred@flintstone.com"],
            [2, "pebbles", "pebbles@flintstone.com"],
        ]

        for row in rows:
            self.execute(
                f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
            )

        records = self.execute(f"SELECT * FROM {self.table_name}")

        assert len(records) == len(rows)
        for i, record in enumerate(records):
            assert record[0] == rows[i][0]

    def test_insert_and_select_many(self):
        keys = [k for k in range(100)]
        rows = []

        random.shuffle(keys)

        for key in keys:
            rows.append([key, f"name-{key}", f"{key}@flintstone.com"])

        for row in rows:
            self.execute(
                f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
            )

        records = self.execute(f"SELECT * FROM {self.table_name}")

        assert len(records) == len(keys)
        keys.sort()
        for i, record in enumerate(records):
            assert record[0] == keys[i]

    @unittest.skip("TODO: No duplicate checking.")
    def test_vm_duplicate_key(self):
        row = (1, "fred", "fred@flintstone.com")
        row_2 = (1, "pebbles", "pebbles@flintstone.com")

        self.execute(
            f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
        )

        with self.assertRaisesRegex(Exception, "users.row_id"):
            self.execute(
                f"INSERT INTO {self.table_name} VALUES ({row_2[0]}, '{row_2[1]}', '{row_2[2]}');"
            )


    @unittest.skip("TODO: table doesnt exist")
    def test_vm_table_not_exists(self):
        pass

