from typing import List, Any, Optional
from toysql.parser import SelectStatement, InsertStatement, CreateStatement, Statement
from toysql.table import Table
from toysql.pager import Pager
from toysql.lexer import StatementLexer
from toysql.parser import Parser
from toysql.lexer import Keyword
import toysql.datatypes as datatypes

from random import randint

SCHEME_TABLE_NAME = "schema"

# TODO this should be called the executor.

# dynamic from disk.
# 1. read schema table from disk
# 2. find table by name and get root_page_number.
# 3. perform function on table. 
 
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
        if self.tables.get(SCHEME_TABLE_NAME):
            return self.tables[SCHEME_TABLE_NAME]

        input = f"CREATE TABLE {SCHEME_TABLE_NAME} (id INT, name TEXT(12), sql_text TEXT(500), root_page_number INT);"

        [statement] = self.parse_input(input)
        return self.load_table(statement, input, 0)

    def get_schema_table(self) -> Optional[Table]:
        return self.get_table(SCHEME_TABLE_NAME)

    def create_table(self, statement: CreateStatement, input:str, root_page_number=None) -> Table:
        if root_page_number is None:
            root_page_number = len(self.pager)

        table_name = statement.table.value
        key = randint(0, 100) # TODO this is a hack - should be auto-incremented.
        self.execute(f"INSERT INTO {SCHEME_TABLE_NAME} VALUES ({key}, '{table_name}', '{input}', {root_page_number});")

        table = self.load_table(statement, input, root_page_number)
        return table


    def load_table(self, statement: CreateStatement, input:str, root_page_number) -> Table:
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

    def get_tables(self):
        return self.execute(f"select * from {SCHEME_TABLE_NAME}")

    def get_table(self, table_name) -> Optional[Table]:
        # TODO we should instead read from internal "tables" table.
        return self.tables[table_name]

    def parse_input(self, input: str) -> List[Any]:
        tokens = self.lexer.lex(input)
        stmts = self.parser.parse(tokens)
        return stmts

    def execute(self, input: str) -> List[Any]:
        stmts = self.parse_input(input)
        results = []

        for stmt in stmts:
            result = self.execute_statement(stmt, input)
            results.append(result)

        return results

    def execute_statement(self, statement: Statement, input: str):
        self.create_schema_table()

        if isinstance(statement, SelectStatement):
            table_name = statement._from.value
            return self.get_table(table_name).select()

        if isinstance(statement, InsertStatement):
            table_name = statement.into.value
            return self.get_table(table_name).insert(statement.values)

        if isinstance(statement, CreateStatement):
            return self.create_table(statement, input)
