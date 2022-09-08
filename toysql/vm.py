from typing import List, Any, Optional
from toysql.parser import SelectStatement, InsertStatement, CreateStatement, Statement
from toysql.table import Table
from toysql.pager import Pager
from toysql.lexer import StatementLexer, Kind
from toysql.record import Record, DataType
from toysql.parser import Parser
from toysql.btree import BTree
from toysql.exceptions import TableFoundException

SCHEME_TABLE_NAME = "schema"


class VM:
    """
    NOTE: this is not yet a real VM with OPcodes etc
    it's simply the glue code between the frontend & backend.
    """

    def __init__(self, file_path):
        self.pager = Pager(file_path)
        self.lexer = StatementLexer()
        self.parser = Parser()
        self.tables = {}
        self.schema_table = self.create_schema_table()
        self.row_counter = 0

    def schema_table_exists(self) -> bool:
        return len(self.pager) > 0

    def create_schema_table(self) -> Table:
        page_number = 0

        if self.schema_table_exists():
            return self.load_table(SCHEME_TABLE_NAME, page_number)

        input = f"CREATE TABLE {SCHEME_TABLE_NAME} (id INT, name TEXT(12), sql_text TEXT(500), root_page_number INT);"
        [statement] = self.parse_input(input)

        if len(self.pager) == 0:
            # If there is no first page create one.
            page_number = self.pager.new()

        return self.create_table(statement, input, page_number)

    def get_schema_table(self) -> Optional[Table]:
        return self.get_table(SCHEME_TABLE_NAME)

    def create_table(
        self, statement: CreateStatement, input: str, root_page_number
    ) -> Table:
        table_name = statement.table.value
        table = self.load_table(table_name, root_page_number)

        if table_name != SCHEME_TABLE_NAME:
            self.schema_table.insert(
                [
                    [DataType.INTEGER, root_page_number],
                    [DataType.TEXT, table_name],
                    [DataType.TEXT, input],
                    [DataType.INTEGER, root_page_number],
                ]
            )

        return table

    def load_table(self, table_name, root_page_number) -> Table:
        tree = BTree(self.pager, root_page_number)
        table = Table(table_name, tree)
        self.tables[table_name] = table
        return table

    def get_table(self, table_name) -> Table:
        if table_name == SCHEME_TABLE_NAME:
            return self.schema_table

        for record in self.schema_table.select():
            if record.values[1][1] == table_name:
                root_page_number = record.values[3][1]
                return self.load_table(table_name, root_page_number)

        raise TableFoundException(f"Table: {table_name} not found")

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

    def insert_statement_to_record(self, statment: InsertStatement) -> Record:
        values = []
        for token in statment.values:
            if token.kind == Kind.integer:
                values.append([DataType.INTEGER, token.value])

            if token.kind == Kind.text:
                values.append([DataType.TEXT, token.value])

        return Record(values)

    def execute_statement(self, statement: Statement, input: str):
        if isinstance(statement, SelectStatement):
            table_name = statement._from.value
            records = [r for r in self.get_table(table_name).select()]
            return records

        if isinstance(statement, InsertStatement):
            table_name = statement.into.value
            record = self.insert_statement_to_record(statement)
            t = self.get_table(table_name)
            t.insert(record)
            return record

        if isinstance(statement, CreateStatement):
            return self.create_table(statement, input, self.pager.new())
