from toysql.statement import InsertStatement, SelectStatement
from toysql.vm import VM


def test_vm(vm: VM):
    # support two operations: inserting a row and printing all rows
    # reside only in memory (no persistence to disk)
    # support a single, hard-coded table
    row = (1, "fred", "fred@flintstone.com")
    row_2 = (2, "pebbles", "pebbles@flintstone.com")

    vm.execute(InsertStatement(row))
    vm.execute(InsertStatement(row_2))
    result = vm.execute(SelectStatement())
    assert result == [row, row_2]
