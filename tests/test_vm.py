from toysql.statement import InsertStatement, SelectStatement
from toysql.vm import VM


def test_vm_one_page(vm: VM):
    row = (1, "fred", "fred@flintstone.com")
    row_2 = (2, "pebbles", "pebbles@flintstone.com")

    vm.execute(InsertStatement(row))
    vm.execute(InsertStatement(row_2))
    result = vm.execute(SelectStatement())
    assert result == [row, row_2]
    # Ensure only 1 page is used.
    assert len(vm.table.pager) == 1


def test_vm_multiple_pages(vm: VM):
    expected_rows = []
    for n in range(50):
        row = (n, f"fred-{n}", f"fred-{n}@flintstone.com")
        expected_rows.append(row)
        vm.execute(InsertStatement(row))

    result = vm.execute(SelectStatement())
    assert result == expected_rows
    # Ensure only 1 page is used.
    assert len(vm.table.pager) == 4
