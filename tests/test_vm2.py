from toysql.vm import VM, SCHEME_TABLE_NAME
from toysql.vmV1 import VM as VMV1
from tests.fixtures import Fixtures
from toysql.parser import Parser
from toysql.lexer import StatementLexer
from toysql.planner import Planner


class TestVM(Fixtures):
    def setUp(self) -> None:
        super().setUp()

        self.vm = VM(self.db_file_path)
        self.table_name = "users"

        self.vm.execute(
            f"CREATE TABLE {self.table_name} (id INT, name TEXT(32), email TEXT(255));"
        )

        self.planner = Planner(self.pager)
        self.lexer = StatementLexer()
        self.parser = Parser()

        def prepare(input: str):
            tokens = self.lexer.lex(input)
            stmts = self.parser.parse(tokens)
            return self.planner.plan(stmts)

        self.prepare = prepare

    # def test_system_schema_table(self):
    #     """
    #     When we init the VM we should auto
    #     create a schema table.
    #     """

    #     assert self.vm.tables[SCHEME_TABLE_NAME]
    #     assert len(self.vm.tables.keys()) == 2
    #     [records] = self.vm.execute(f"SELECT * FROM {SCHEME_TABLE_NAME}")
    #     assert len(records) == 1
    #     assert records[0][1] == self.table_name

    def test_insert_and_select(self):

        vmv1 = VMV1(self.pager)
        rows = [
            [1, "fred", "fred@flintstone.com"],
            [2, "pebbles", "pebbles@flintstone.com"],
        ]

        for row in rows:
            self.vm.execute(
                f"INSERT INTO {self.table_name} VALUES ({row[0]}, '{row[1]}', '{row[2]}');"
            )

        program = self.prepare(f"SELECT * FROM {self.table_name}")
        [records] = vmv1.execute(program)

        assert len(records) == len(rows)
        for i, record in enumerate(records):
            assert record[0] == rows[i][0]
