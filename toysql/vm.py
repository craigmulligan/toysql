from typing import Union
from toysql.statement import SelectStatement, InsertStatement
from toysql.table import Table
from toysql.pager import Pager


class VM:
    def __init__(self, file_path):
        self.pager = Pager(file_path)
        self.table = Table(self.pager)

    def execute(self, statement: Union[SelectStatement, InsertStatement]):
        if isinstance(statement, SelectStatement):
            return self.table.select()

        if isinstance(statement, InsertStatement):
            return self.table.insert(statement.row)
