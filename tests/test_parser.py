from toysql.lexer import Token, Kind, Location
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
    def test_found_x(self):
        create = [
            Token("create", Kind.keyword),
            Token("table", Kind.keyword),
            Token("users", Kind.identifier),
            Token("(", Kind.symbol),
            Token("id", Kind.identifier),
            Token("int", Kind.keyword),
            Token(",", Kind.symbol),
            Token("name", Kind.identifier),
            Token("text", Kind.keyword),
            Token("(", Kind.symbol),
            Token(255, Kind.integer),
            Token(")", Kind.symbol),
            Token(")", Kind.symbol),
            Token(";", Kind.symbol),
        ]
        insert = [
            Token("insert", Kind.keyword),
            Token("into", Kind.keyword),
            Token("users", Kind.identifier),
            Token("values", Kind.keyword),
            Token("(", Kind.symbol),
            Token(1, Kind.integer),
            Token(",", Kind.symbol),
            Token("Phil", Kind.text),
            Token(")", Kind.symbol),
            Token(";", Kind.symbol),
        ]
        select = [
            Token("select", Kind.keyword),
            Token("a", Kind.identifier),
            Token("from", Kind.keyword),
            Token("my_table", Kind.identifier),
            Token(";", Kind.symbol),
        ]

        tokens = create + insert + select
        stmts = parse(tokens)
        assert isinstance(stmts[0], CreateStatement)
        assert isinstance(stmts[1], InsertStatement)
        assert isinstance(stmts[2], SelectStatement)

    def test_found_not_terminator(self):
        tokens = [
            Token("select", Kind.keyword),
            Token("*", Kind.identifier),
            Token("from", Kind.keyword),
            Token("my_table", Kind.identifier),
        ]
        [stmt] = parse(tokens)
        assert isinstance(stmt, SelectStatement)
        assert stmt._from.value == "my_table"
        assert stmt.items[0].value == "*"


class TestCreateParser(TestCase):
    def test_found(self):
        tokens = [
            Token("create", Kind.keyword),
            Token("table", Kind.keyword),
            Token("users", Kind.identifier),
            Token("(", Kind.symbol),
            Token("id", Kind.identifier),
            Token("int", Kind.keyword),
            Token(",", Kind.symbol),
            Token("name", Kind.identifier),
            Token("text", Kind.keyword),
            Token("(", Kind.symbol),
            Token(255, Kind.integer),
            Token(")", Kind.symbol),
            Token(")", Kind.symbol),
            Token(";", Kind.symbol),
        ]
        cursor = TokenCursor(tokens)
        stmt = CreateStatement.parse(cursor)
        assert isinstance(stmt, CreateStatement)
        assert stmt.columns == [
            ColumnDefinition(tokens[4], tokens[5], None),
            ColumnDefinition(tokens[7], tokens[8], tokens[10]),
        ]
        assert stmt.table == tokens[2]


class TestInsertParser(TestCase):
    def test_found(self):
        tokens = [
            Token("insert", Kind.keyword),
            Token("into", Kind.keyword),
            Token("users", Kind.identifier),
            Token("values", Kind.keyword),
            Token("(", Kind.symbol),
            Token(1, Kind.integer),
            Token(",", Kind.symbol),
            Token("Phil", Kind.text),
            Token(")", Kind.symbol),
            Token(";", Kind.symbol),
        ]
        cursor = TokenCursor(tokens)
        stmt = InsertStatement.parse(cursor)
        assert isinstance(stmt, InsertStatement)
        assert stmt.columns == []
        assert stmt.values == [tokens[5], tokens[7]]
        assert stmt.into == tokens[2]


class TestSelectParser(TestCase):
    def test_found_astrix(self):
        tokens = [
            Token("select", Kind.keyword),
            Token("*", Kind.symbol),
            Token("from", Kind.keyword),
            Token("my_table", Kind.identifier),
            Token("where", Kind.keyword),
            Token("x", Kind.identifier),
            Token("=", Kind.symbol),
            Token("hi", Kind.text),
            Token("and", Kind.keyword),
            Token("y", Kind.identifier),
            Token("=", Kind.symbol),
            Token(123, Kind.integer),
            Token(";", Kind.symbol),
        ]
        cursor = TokenCursor(tokens)
        stmt = SelectStatement.parse(cursor)
        assert isinstance(stmt, SelectStatement)
        assert stmt._from.value == "my_table"
        assert stmt.items[0].value == "*"

    def test_multi_columns(self):
        tokens = [
            Token("select", Kind.keyword),
            Token("a", Kind.identifier),
            Token(",", Kind.symbol),
            Token("b", Kind.identifier),
            Token("from", Kind.keyword),
            Token("my_table", Kind.identifier),
            Token("where", Kind.keyword),
            Token("x", Kind.identifier),
            Token("=", Kind.symbol),
            Token("hi", Kind.text),
            Token("and", Kind.keyword),
            Token("y", Kind.identifier),
            Token("=", Kind.symbol),
            Token(123, Kind.integer),
            Token(";", Kind.symbol),
        ]
        cursor = TokenCursor(tokens)
        stmt = SelectStatement.parse(cursor)
        assert isinstance(stmt, SelectStatement)
        assert stmt._from.value == "my_table"
        assert stmt.items[0].value == "a"
        assert stmt.items[1].value == "b"

    def test_found(self):
        tokens = [
            Token("select", Kind.keyword),
            Token("a", Kind.identifier),
            Token("from", Kind.keyword),
            Token("my_table", Kind.identifier),
            Token("where", Kind.keyword),
            Token("x", Kind.identifier),
            Token("=", Kind.symbol),
            Token("hi", Kind.text),
            Token("and", Kind.keyword),
            Token("y", Kind.identifier),
            Token("=", Kind.symbol),
            Token(123, Kind.integer),
            Token(";", Kind.symbol),
        ]
        cursor = TokenCursor(tokens)
        stmt = SelectStatement.parse(cursor)
        assert isinstance(stmt, SelectStatement)
        assert stmt._from.value == "my_table"
        assert stmt.items[0].value == "a"

    def test_not_found(self):
        tokens = [
            Token("insert", Kind.keyword),
            Token("into", Kind.keyword),
            Token("users", Kind.identifier),
            Token("values", Kind.keyword),
            Token("(", Kind.symbol),
            Token(1, Kind.integer),
            Token(",", Kind.symbol),
            Token("Phil", Kind.text),
            Token(")", Kind.symbol),
            Token(";", Kind.symbol),
        ]
        cursor = TokenCursor(tokens)

        with self.assertRaises(LookupError):
            SelectStatement.parse(cursor)

        assert cursor.pointer == 0
