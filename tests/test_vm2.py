from toysql.vm import VM
from toysql.vmV1 import VM as VMV1
from tests.fixtures import Fixtures
from toysql.compiler import Compiler, SCHEMA_TABLE_NAME


class TestVM(Fixtures):
    def setUp(self) -> None:
        super().setUp()

        self.vm = VM(self.db_file_path)
        self.table_name = "users"

        self.vm.execute(
            f"CREATE TABLE {self.table_name} (id INT, name TEXT(32), email TEXT(255));"
        )

        self.root_page_number = 1

    def test_create(self):
        """
        When we init the VM we should auto
        create a schema table.
        """
        vm = VMV1(self.pager)
        table_name = "org"
        create_stmt = f"CREATE TABLE {table_name} (id INT, name TEXT);"
        program = Compiler(self.pager, vm).compile(create_stmt)
        [row for row in vm.execute(program)]

        program = Compiler(self.pager, vm).compile(f"SELECT * FROM {SCHEMA_TABLE_NAME}")
        records = [r for r in vm.execute(program)]

        assert len(records) == 2
        new_row = records[1]
        assert new_row == [2, table_name, create_stmt, len(self.pager) - 1]

    def test_insert_and_select(self):
        vmv1 = VMV1(self.pager)

        rows = [
            [1, "fred", "fred@flintstone.com"],
            [2, "pebbles", "pebbles@flintstone.com"],
        ]

        for row in rows:
            prog = Compiler(self.pager, vmv1).compile(
                f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
            )
            [row for row in vmv1.execute(prog)]

        program = Compiler(self.pager, vmv1).compile(f"SELECT * FROM {self.table_name}")

        records = [r for r in vmv1.execute(program)]

        assert len(records) == len(rows)
        for i, record in enumerate(records):
            assert record[0] == rows[i][0]
