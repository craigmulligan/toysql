from toysql.statement import InsertStatement, SelectStatement
from toysql.vm import VM
from toysql.exceptions import DuplicateKeyException
from tests.fixtures import Fixtures


class TestTree(Fixtures):
    def test_vm_one_page_x(self):
        vm = self.vm
        row = (1, "fred", "fred@flintstone.com")
        row_2 = (2, "pebbles", "pebbles@flintstone.com")

        vm.execute(InsertStatement(row))
        vm.execute(InsertStatement(row_2))
        result = vm.execute(SelectStatement())
        assert result == [row, row_2]

        # Ensure only 1 page is used.
        assert len(vm.table.pager) == 1

    def test_vm_one_page_out_of_order(self):
        vm: VM = self.vm
        row = (1, "fred", "fred@flintstone.com")
        row_2 = (2, "pebbles", "pebbles@flintstone.com")

        vm.execute(InsertStatement(row_2))
        vm.execute(InsertStatement(row))
        result = vm.execute(SelectStatement())
        assert result == [row, row_2]

    def test_vm_one_page_duplicate_key(self):
        vm: VM = self.vm
        row = (1, "fred", "fred@flintstone.com")
        row_2 = (1, "pebbles", "pebbles@flintstone.com")

        vm.execute(InsertStatement(row))

        with self.assertRaises(DuplicateKeyException):
            vm.execute(InsertStatement(row_2))

    def test_vm_multiple_pages(self):
        vm: VM = self.vm
        expected_rows = []
        for n in range(50):
            row = (n, f"fred-{n}", f"fred-{n}@flintstone.com")
            expected_rows.append(row)
            vm.execute(InsertStatement(row))

        result = vm.execute(SelectStatement())
        assert result == expected_rows

    def test_retains_state_on_disk(self):
        vm: VM = self.vm
        db_file_path = self.db_file_path

        expected_rows = []
        for n in range(13):
            row = (n, f"fred-{n}", f"fred-{n}@flintstone.com")
            expected_rows.append(row)
            vm.execute(InsertStatement(row))

        vm2 = VM(db_file_path)
        # Should read from same db.
        result = vm2.execute(SelectStatement())

        assert result == expected_rows
        assert vm.table.tree.show() == vm2.table.tree.show()
