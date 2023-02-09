from toysql.lexer import Token, Keyword, Symbol, Identifier, DataType
from toysql.parser import (
    parse,
    SelectStatement,
    InsertStatement,
    CreateStatement,
    TokenCursor,
    ColumnDefinition,
)
from unittest import TestCase


class TestParser(TestCase):
    def test_found_multi_statement(self):
        create = [
            Token(Keyword.create),
            Token(Keyword.table),
            Token(Identifier.long, value="users"),
            Token(Symbol.left_paren),
            Token(Identifier.long, value="id"),
            Token(Keyword.integer),
            Token(Symbol.comma),
            Token(Identifier.long, value="name"),
            Token(Keyword.text),
            Token(Symbol.right_paren),
            Token(Symbol.semicolon),
        ]
        insert = [
            Token(Keyword.insert),
            Token(Keyword.into),
            Token(Identifier.long, value="users"),
            Token(Keyword.values),
            Token(Symbol.left_paren),
            Token(DataType.integer, value="1"),
            Token(Symbol.comma),
            Token(DataType.text, value="Phil"),
            Token(Symbol.right_paren),
            Token(Symbol.semicolon),
        ]
        select = [
            Token(Keyword.select),
            Token(Identifier.long, value="a"),
            Token(Keyword._from),
            Token(Identifier.long, value="my_table"),
            Token(Symbol.semicolon),
        ]

        tokens = create + insert + select
        stmts = parse(tokens)

        assert len(stmts) == 3
        assert isinstance(stmts[0], CreateStatement)
        assert isinstance(stmts[1], InsertStatement)
        assert isinstance(stmts[2], SelectStatement)

    def test_found_not_terminator(self):
        tokens = [
            Token(Keyword.select),
            Token(Symbol.asterisk),
            Token(Keyword._from),
            Token(Identifier.long, value="my_table"),
        ]

        [stmt] = parse(tokens)
        assert isinstance(stmt, SelectStatement)
        assert stmt._from.value == "my_table"
        assert stmt.items[0].value == "*"


class TestCreateParser(TestCase):
    def test_create(self):
        tokens = [
            Token(Keyword.create),
            Token(Keyword.table),
            Token(Identifier.long, value="users"),
            Token(Symbol.left_paren),
            Token(Identifier.long, value="id"),
            Token(Keyword.integer),
            Token(Keyword.primary),
            Token(Keyword.key),
            Token(Symbol.comma),
            Token(Identifier.long, value="name"),
            Token(Keyword.text),
            Token(Symbol.right_paren),
            Token(Symbol.semicolon),
        ]

        cursor = TokenCursor(tokens)
        stmt = CreateStatement.parse(cursor)
        assert isinstance(stmt, CreateStatement)
        assert stmt.columns == [
            ColumnDefinition(
                Token(Identifier.long, value="id"),
                Token(Keyword.integer),
                is_primary_key=True,
            ),
            ColumnDefinition(
                Token(Identifier.long, value="name"),
                Token(Keyword.text),
                is_primary_key=False,
            ),
        ]
        assert stmt.table == tokens[2]


class TestInsertParser(TestCase):
    def test_insert(self):
        tokens = [
            Token(Keyword.insert),
            Token(Keyword.into),
            Token(Identifier.long, value="users"),
            Token(Keyword.values),
            Token(Symbol.left_paren),
            Token(DataType.integer, value="1"),
            Token(Symbol.comma),
            Token(DataType.text, value="Phil"),
            Token(Symbol.right_paren),
            Token(Symbol.semicolon),
        ]
        cursor = TokenCursor(tokens)
        stmt = InsertStatement.parse(cursor)
        assert isinstance(stmt, InsertStatement)
        assert stmt.columns == []
        assert stmt.values == [tokens[5], tokens[7]]
        assert stmt.into == tokens[2]


class TestSelectParser(TestCase):
    def test_select_astrix(self):
        tokens = [
            Token(Keyword.select),
            Token(Symbol.asterisk),
            Token(Keyword._from),
            Token(Identifier.long, value="my_table"),
            Token(Keyword.where),
            Token(Identifier.long, value="x"),
            Token(Symbol.equal),
            Token(DataType.text, value="hi"),
            Token(Keyword._and),
            Token(Identifier.long, value="y"),
            Token(Symbol.equal),
            Token(DataType.integer, value="123"),
            Token(Symbol.semicolon),
        ]
        cursor = TokenCursor(tokens)
        stmt = SelectStatement.parse(cursor)
        assert isinstance(stmt, SelectStatement)
        assert stmt._from.value == "my_table"
        assert stmt.items[0].value == "*"

    def test_select_multi_columns(self):
        tokens = [
            Token(Keyword.select),
            Token(Identifier.long, value="a"),
            Token(Symbol.comma),
            Token(Identifier.long, value="b"),
            Token(Keyword._from),
            Token(Identifier.long, value="my_table"),
            Token(Keyword.where),
            Token(Identifier.long, value="x"),
            Token(Symbol.equal),
            Token(DataType.text, value="hi"),
            Token(Keyword._and),
            Token(Identifier.long, value="y"),
            Token(Symbol.equal),
            Token(DataType.integer, value="123"),
            Token(Symbol.semicolon),
        ]
        cursor = TokenCursor(tokens)
        stmt = SelectStatement.parse(cursor)
        assert isinstance(stmt, SelectStatement)
        assert stmt._from.value == "my_table"
        assert stmt.items[0].value == "a"
        assert stmt.items[1].value == "b"

    def test_insert_not_found(self):
        tokens = [
            Token(Keyword.insert),
            Token(Keyword.into),
            Token(Identifier.long, value="users"),
            Token(Keyword.values),
            Token(Symbol.right_paren),
            Token(DataType.integer, value="1"),
            Token(Symbol.comma),
            Token(DataType.text, value="Phil"),
            Token(Symbol.left_paren),
            Token(Symbol.semicolon),
        ]
        cursor = TokenCursor(tokens)

        with self.assertRaises(LookupError):
            SelectStatement.parse(cursor)

        assert cursor.pointer == 0
