from typing import List, Any
from toysql.parser import SelectStatement, InsertStatement, CreateStatement, Statement
from toysql.table import Table
from toysql.pager import Pager
from toysql.lexer import StatementLexer
from toysql.parser import Parser
from toysql.lexer import Keyword
import toysql.datatypes as datatypes

SCHEME_TABLE_NAME = "schema"

# TODO this should be called the executor.
class VM:
    def __init__(self, file_path):
        self.pager = Pager(file_path)
        self.lexer = StatementLexer()
        self.parser = Parser()
        self.tables = {}

    def create_schema_table(self) -> Table:
        # TODO we need to load the schema (cols) from disk.
        # sqlite uses txt.
        # TODO: The string datatypes should be variable lengths. 
        input = f"CREATE TABLE {SCHEME_TABLE_NAME} (id INT, name TEXT(12), email TEXT(500));"
        tokens = self.lexer.lex(input)
        [statement]= self.parser.parse(tokens)
        return self.create_table(statement, 0)

    def get_schema_table(self) -> Table:
        return self.get_table(SCHEME_TABLE_NAME) 

    def create_table(self, statement: CreateStatement, root_page_number=None) -> Table:
        if root_page_number is None: 
            root_page_number = len(self.pager)

        table_name = statement.table.value
        columns = {}
        for col in statement.columns:
            length = col.length.value if col.length else None
            columns[col.name.value] = datatypes.factory(
                Keyword(col.datatype.value), length
            )
        table = Table(self.pager, columns, root_page_number)
        self.tables[table_name] = table
        return table

    def get_table(self, table_name) -> Table:
        # TODO we should instead read from internal "tables" table.
        results = self.execute(f"select * from {SCHEME_TABLE_NAME}")
        print(results)
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
            return self.create_table(statement)
