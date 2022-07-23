from toysql.parser import InsertStatement, SelectStatement
from toysql.vm import VM
from toysql.exceptions import DuplicateKeyException
from tests.fixtures import Fixtures
import toysql.datatypes as datatypes


class TestTree(Fixtures):
    def setUp(self) -> None:
        super().setUp()
        self.vm: VM
        self.table_name = "users"
        self.vm.execute(
            f"CREATE TABLE {self.table_name} (id INT, name TEXT(32), email TEXT(255));"
        )

    def test_vm_one_page_x(self):
        rows = [
            [1, "fred", "fred@flintstone.com"],
            [2, "pebbles", "pebbles@flintstone.com"],
        ]

        for row in rows:
            self.vm.execute(
                f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
            )

        [result] = self.vm.execute(f"SELECT * FROM {self.table_name}")
        assert result == rows

        # Ensure only 1 page is used.
        assert len(self.vm.pager) == 1

        table = self.vm.get_table(self.table_name)
        column_names = [*table.columns]
        assert column_names == ["id", "name", "email"]

    def test_vm_one_page_out_of_order(self):
        self.table_name = "users"
        rows = [
            [1, "fred", "fred@flintstone.com"],
            [2, "pebbles", "pebbles@flintstone.com"],
        ]
        # Enure they are out of order.
        ordered_rows = rows.copy()
        rows.reverse()
        for row in rows:
            self.vm.execute(
                f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
            )

        [result] = self.vm.execute(f"SELECT * FROM {self.table_name}")

        assert result == ordered_rows

    def test_vm_one_page_duplicate_key(self):
        row = (1, "fred", "fred@flintstone.com")
        row_2 = (1, "pebbles", "pebbles@flintstone.com")
        self.vm.execute(
            f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
        )

        with self.assertRaises(DuplicateKeyException):
            self.vm.execute(
                f"INSERT INTO {self.table_name} VALUES ({row_2[0]}, '{row_2[1]}', '{row_2[2]}');"
            )

    def test_vm_multiple_pages(self):
        expected_rows = []

        for n in range(50):
            row = (n, f"fred-{n}", f"fred-{n}@flintstone.com")
            expected_rows.append(row)
            self.vm.execute(
                f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
            )


        [result] = self.vm.execute(f"SELECT * FROM {self.table_name}")
        assert result == expected_rows

    def test_retains_state_on_disk(self):
        db_file_path = self.db_file_path
        expected_rows = []

        for n in range(13):
            row = (n, f"fred-{n}", f"fred-{n}@flintstone.com")
            expected_rows.append(row)
            self.vm.execute(
                f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
            )

        
        self.vm2 = VM(db_file_path)

        # TODO shouldn't need to create the table for it it register.
        self.vm2.execute(
            f"CREATE TABLE {self.table_name} (id INT, name TEXT(32), email TEXT(255));"
        )
        # Should read from same db.
        [result] = self.vm2.execute(f"SELECT * FROM {self.table_name}")

        assert result == expected_rows
        assert self.vm.tables[self.table_name].tree.show() == self.vm2.tables[self.table_name].tree.show()


    def test_multiple_tables(self):
        table_name_2 = "birds"  
        row = [1, "tit"]
        self.vm.execute(
            f"CREATE TABLE {table_name_2} (id INT, name TEXT(32));"
        )
        self.vm.execute(
            f"INSERT INTO {table_name_2} VALUES ({row[0]}, '{row[1]}');"
        )

        [result] = self.vm.execute(f"SELECT * FROM {table_name_2}")
        assert result == [row]
