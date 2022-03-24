from typing import Union
from toysql.statement import SelectStatement, InsertStatement
from toysql.table import Table


class VM:
    def __init__(self):
        self.table = Table()

    def execute(self, statement: Union[SelectStatement, InsertStatement]):
        if isinstance(statement, SelectStatement):
            return self.table.select()

        if isinstance(statement, InsertStatement):
            return self.table.insert(statement.row)
