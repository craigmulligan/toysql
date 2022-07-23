from typing import Union, List, Any
from toysql.parser import SelectStatement, InsertStatement, CreateStatement, Statement
from toysql.table import Table
from toysql.pager import Pager
from toysql.lexer import StatementLexer
from toysql.parser import Parser
from toysql.lexer import Keyword
import toysql.datatypes as datatypes


class VM:
    def __init__(self, file_path):
        self.pager = Pager(file_path)
        self.lexer = StatementLexer()
        self.parser = Parser()
        self.tables = {}

    def create_table(self, table_name, columns) -> Table:
        table = Table(self.pager, columns)
        self.tables[table_name] = table
        return table

    def get_table(self, table_name) -> Table:
        # TODO we should instead read from the "tables" internal table.
        return self.tables[table_name]

    def execute(self, input: str) -> List[Any]:
        tokens = self.lexer.lex(input)
        stmts = self.parser.parse(tokens)
        results = []

        for stmt in stmts:
            result = self.execute_statement(stmt)
            results.append(result)

        return results

    def execute_statement(self, statement: Statement):
        if isinstance(statement, SelectStatement):
            table_name = statement._from.value
            return self.get_table(table_name).select()

        if isinstance(statement, InsertStatement):
            table_name = statement.into.value
            return self.get_table(table_name).insert(statement.values)

        if isinstance(statement, CreateStatement):
            table_name = statement.table.value

            columns = {}
            for col in statement.columns:
                length = col.length.value if col.length else None
                columns[col.name.value] = datatypes.factory(
                    Keyword(col.datatype.value), length
                )

            return self.create_table(table_name, columns)
