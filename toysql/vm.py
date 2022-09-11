from typing import List, Any, Optional, Dict
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

    def get_table_columns(self, table):
        [statement] = self.parse_input(table.input)
        return statement.columns

    def get_table_column_names(self, table):
        columns = self.get_table_columns(table)
        names = []
        for col in columns:
            if col.name.value != "*":
                names.append(col.name.value)

        return names

    def schema_table_exists(self) -> bool:
        return len(self.pager) > 0

    def create_schema_table(self) -> Table:
        page_number = 0
        input = f"CREATE TABLE {SCHEME_TABLE_NAME} (id INT, name text(12), sql_text text(500), root_page_number INT);"

        if self.schema_table_exists():
            return self.load_table(SCHEME_TABLE_NAME, input, page_number)

        input = f"CREATE TABLE {SCHEME_TABLE_NAME} (id INT, name text(12), sql_text text(500), root_page_number INT);"
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
        table = self.load_table(table_name, input, root_page_number)

        if table_name != SCHEME_TABLE_NAME:
            self.schema_table.insert(
                [
                    [DataType.integer, root_page_number],
                    [DataType.text, table_name],
                    [DataType.text, input],
                    [DataType.integer, root_page_number],
                ]
            )

        return table

    def load_table(self, table_name, input, root_page_number) -> Table:
        tree = BTree(self.pager, root_page_number)
        table = Table(table_name, input, tree)
        self.tables[table_name] = table
        return table

    def get_table(self, table_name) -> Table:
        if table_name == SCHEME_TABLE_NAME:
            return self.schema_table

        for record in self.schema_table.select():
            if record.values[1][1] == table_name:
                root_page_number = record.values[3][1]
                input = record.values[2][1]
                return self.load_table(table_name, input, root_page_number)

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
                values.append([DataType.integer, token.value])

            if token.kind == Kind.text:
                values.append([DataType.text, token.value])

        return Record(values)

    def execute_statement(self, statement: Statement, input: str):
        if isinstance(statement, SelectStatement):
            table_name = statement._from.value
            table = self.get_table(table_name)

            column_index = []
            column_names = self.get_table_column_names(table)

            for column_name in statement.items:
                if column_name.value == "*":
                    # TODO need to handle this better.
                    break

                column_index.append(column_names.index(column_name.value))

            if len(column_index) == 0:
                # If no columns we select every index.
                column_index = list(range(0, len(column_names)))

            results = []
            for r in table.select():
                result = []
                if len(column_index):
                    for i in column_index:
                        result.append(r.values[i][1])
                results.append(result)

            return results

        if isinstance(statement, InsertStatement):
            table_name = statement.into.value
            record = self.insert_statement_to_record(statement)
            t = self.get_table(table_name)
            t.insert(record)
            return record

        if isinstance(statement, CreateStatement):
            return self.create_table(statement, input, self.pager.new())
