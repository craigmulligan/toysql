from toysql.parser import InsertStatement, SelectStatement
from toysql.vm import VM, SCHEME_TABLE_NAME
from toysql.exceptions import DuplicateKeyException
from toysql.pager import Pager
from tests.fixtures import Fixtures
from unittest import TestCase

import tempfile
import toysql.datatypes as datatypes


class TestVM(TestCase):
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

    def test_system_schema_table(self):
        """
        When we init the VM we should auto
        create a schema table.
        """

        vm = VM(self.db_file_path)
        table_name = "users"

        assert vm.tables[SCHEME_TABLE_NAME]
        [table] = vm.execute(
            f"CREATE TABLE users (id INT, name TEXT(32), email TEXT(255));"
        )

        assert len(vm.tables.keys()) == 2
        [records] = vm.execute(f"SELECT * FROM {SCHEME_TABLE_NAME}")

        assert records[0].values[1][1] == table_name

        rows = [
            [1, "fred", "fred@flintstone.com"],
            [2, "pebbles", "pebbles@flintstone.com"],
        ]

        for row in rows:
            vm.execute(
                f"INSERT INTO {table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
            )

        [records] = vm.execute(f"SELECT * FROM {table_name}")
        for i, record in enumerate(records):
            assert record.row_id == rows[i][0]

        # assert False

    # self.vm: VM
    # self.table_name = "users"
    # self.vm.execute(
    #     f"CREATE TABLE {self.table_name} (id INT, name TEXT(32), email TEXT(255));"
    # )

    # def test_vm_one_page_x(self):
    #     rows = [
    #         [1, "fred", "fred@flintstone.com"],
    #         [2, "pebbles", "pebbles@flintstone.com"],
    #     ]

    #     for row in rows:
    #         self.vm.execute(
    #             f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
    #         )

    #     [result] = self.vm.execute(f"SELECT * FROM {self.table_name}")
    #     assert result == rows

    #     # Ensure only 1 page is used.
    #     assert len(self.vm.pager) == 1

    #     table = self.vm.get_table(self.table_name)
    #     column_names = [*table.columns]
    #     assert column_names == ["id", "name", "email"]

    # def test_vm_one_page_out_of_order(self):
    #     self.table_name = "users"
    #     rows = [
    #         [1, "fred", "fred@flintstone.com"],
    #         [2, "pebbles", "pebbles@flintstone.com"],
    #     ]
    #     # Enure they are out of order.
    #     ordered_rows = rows.copy()
    #     rows.reverse()
    #     for row in rows:
    #         self.vm.execute(
    #             f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
    #         )

    #     [result] = self.vm.execute(f"SELECT * FROM {self.table_name}")

    #     assert result == ordered_rows

    # def test_vm_one_page_duplicate_key(self):
    #     row = (1, "fred", "fred@flintstone.com")
    #     row_2 = (1, "pebbles", "pebbles@flintstone.com")
    #     self.vm.execute(
    #         f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
    #     )

    #     with self.assertRaises(DuplicateKeyException):
    #         self.vm.execute(
    #             f"INSERT INTO {self.table_name} VALUES ({row_2[0]}, '{row_2[1]}', '{row_2[2]}');"
    #         )

    # def test_vm_multiple_pages(self):
    #     expected_rows = []

    #     for n in range(50):
    #         row = (n, f"fred-{n}", f"fred-{n}@flintstone.com")
    #         expected_rows.append(row)
    #         self.vm.execute(
    #             f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
    #         )

    #     [result] = self.vm.execute(f"SELECT * FROM {self.table_name}")
    #     assert result == expected_rows

    # def test_retains_state_on_disk(self):
    #     db_file_path = self.db_file_path
    #     expected_rows = []

    #     for n in range(13):
    #         row = (n, f"fred-{n}", f"fred-{n}@flintstone.com")
    #         expected_rows.append(row)
    #         self.vm.execute(
    #             f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
    #         )

    #
    #     self.vm2 = VM(db_file_path)

    #     # TODO shouldn't need to create the table for it it register.
    #     self.vm2.execute(
    #         f"CREATE TABLE {self.table_name} (id INT, name TEXT(32), email TEXT(255));"
    #     )
    #     # Should read from same db.
    #     [result] = self.vm2.execute(f"SELECT * FROM {self.table_name}")

    #     assert result == expected_rows
    #     assert self.vm.tables[self.table_name].tree.show() == self.vm2.tables[self.table_name].tree.show()

    # def test_multiple_tables(self):
    #     table_name_2 = "birds"
    #     row = [1, "tit"]
    #     self.vm.execute(
    #         f"CREATE TABLE {table_name_2} (id INT, name TEXT(32));"
    #     )
    #     self.vm.execute(
    #         f"INSERT INTO {table_name_2} VALUES ({row[0]}, '{row[1]}');"
    #     )

    #     [result] = self.vm.execute(f"SELECT * FROM {table_name_2}")
    #     assert result == [row]
