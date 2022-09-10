from toysql.lexer import Token, Kind, Location
from toysql.parser import (
    Parser,
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
            Token("create", Kind.keyword, Location(0, 0)),
            Token("table", Kind.keyword, Location(0, 0)),
            Token("users", Kind.identifier, Location(0, 0)),
            Token("(", Kind.symbol, Location(0, 0)),
            Token("id", Kind.identifier, Location(0, 0)),
            Token("int", Kind.keyword, Location(0, 0)),
            Token(",", Kind.symbol, Location(0, 0)),
            Token("name", Kind.identifier, Location(0, 0)),
            Token("text", Kind.keyword, Location(0, 0)),
            Token("(", Kind.symbol, Location(0, 0)),
            Token(255, Kind.integer, Location(0, 0)),
            Token(")", Kind.symbol, Location(0, 0)),
            Token(")", Kind.symbol, Location(0, 0)),
            Token(";", Kind.symbol, Location(0, 0)),
        ]
        insert = [
            Token("insert", Kind.keyword, Location(0, 0)),
            Token("into", Kind.keyword, Location(0, 0)),
            Token("users", Kind.identifier, Location(0, 0)),
            Token("values", Kind.keyword, Location(0, 0)),
            Token("(", Kind.symbol, Location(0, 0)),
            Token(1, Kind.integer, Location(0, 0)),
            Token(",", Kind.symbol, Location(0, 0)),
            Token("Phil", Kind.text, Location(0, 0)),
            Token(")", Kind.symbol, Location(0, 0)),
            Token(";", Kind.symbol, Location(0, 0)),
        ]
        select = [
            Token("select", Kind.keyword, Location(0, 0)),
            Token("a", Kind.identifier, Location(0, 0)),
            Token("from", Kind.keyword, Location(0, 0)),
            Token("my_table", Kind.identifier, Location(0, 0)),
            Token(";", Kind.symbol, Location(0, 0)),
        ]

        tokens = create + insert + select
        stmts = Parser().parse(tokens)
        assert isinstance(stmts[0], CreateStatement)
        assert isinstance(stmts[1], InsertStatement)
        assert isinstance(stmts[2], SelectStatement)

    def test_found_not_terminator(self):
        tokens = [
            Token("select", Kind.keyword, Location(0, 0)),
            Token("*", Kind.identifier, Location(0, 0)),
            Token("from", Kind.keyword, Location(0, 0)),
            Token("my_table", Kind.identifier, Location(0, 0)),
        ]
        [stmt] = Parser().parse(tokens)
        assert isinstance(stmt, SelectStatement)
        assert stmt._from.value == "my_table"
        assert stmt.items[0].value == "*"


class TestCreateParser(TestCase):
    def test_found(self):
        tokens = [
            Token("create", Kind.keyword, Location(0, 0)),
            Token("table", Kind.keyword, Location(0, 0)),
            Token("users", Kind.identifier, Location(0, 0)),
            Token("(", Kind.symbol, Location(0, 0)),
            Token("id", Kind.identifier, Location(0, 0)),
            Token("int", Kind.keyword, Location(0, 0)),
            Token(",", Kind.symbol, Location(0, 0)),
            Token("name", Kind.identifier, Location(0, 0)),
            Token("text", Kind.keyword, Location(0, 0)),
            Token("(", Kind.symbol, Location(0, 0)),
            Token(255, Kind.integer, Location(0, 0)),
            Token(")", Kind.symbol, Location(0, 0)),
            Token(")", Kind.symbol, Location(0, 0)),
            Token(";", Kind.symbol, Location(0, 0)),
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
            Token("insert", Kind.keyword, Location(0, 0)),
            Token("into", Kind.keyword, Location(0, 0)),
            Token("users", Kind.identifier, Location(0, 0)),
            Token("values", Kind.keyword, Location(0, 0)),
            Token("(", Kind.symbol, Location(0, 0)),
            Token(1, Kind.integer, Location(0, 0)),
            Token(",", Kind.symbol, Location(0, 0)),
            Token("Phil", Kind.text, Location(0, 0)),
            Token(")", Kind.symbol, Location(0, 0)),
            Token(";", Kind.symbol, Location(0, 0)),
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
            Token("select", Kind.keyword, Location(0, 0)),
            Token("*", Kind.symbol, Location(0, 0)),
            Token("from", Kind.keyword, Location(0, 0)),
            Token("my_table", Kind.identifier, Location(0, 0)),
            Token("where", Kind.keyword, Location(0, 0)),
            Token("x", Kind.identifier, Location(0, 0)),
            Token("=", Kind.symbol, Location(0, 0)),
            Token("hi", Kind.text, Location(0, 0)),
            Token("and", Kind.keyword, Location(0, 0)),
            Token("y", Kind.identifier, Location(0, 0)),
            Token("=", Kind.symbol, Location(0, 0)),
            Token(123, Kind.integer, Location(0, 0)),
            Token(";", Kind.symbol, Location(0, 0)),
        ]
        cursor = TokenCursor(tokens)
        stmt = SelectStatement.parse(cursor)
        assert isinstance(stmt, SelectStatement)
        assert stmt._from.value == "my_table"
        assert stmt.items[0].value == "*"

    def test_multi_columns(self):
        tokens = [
            Token("select", Kind.keyword, Location(0, 0)),
            Token("a", Kind.identifier, Location(0, 0)),
            Token(",", Kind.symbol, Location(0, 0)),
            Token("b", Kind.identifier, Location(0, 0)),
            Token("from", Kind.keyword, Location(0, 0)),
            Token("my_table", Kind.identifier, Location(0, 0)),
            Token("where", Kind.keyword, Location(0, 0)),
            Token("x", Kind.identifier, Location(0, 0)),
            Token("=", Kind.symbol, Location(0, 0)),
            Token("hi", Kind.text, Location(0, 0)),
            Token("and", Kind.keyword, Location(0, 0)),
            Token("y", Kind.identifier, Location(0, 0)),
            Token("=", Kind.symbol, Location(0, 0)),
            Token(123, Kind.integer, Location(0, 0)),
            Token(";", Kind.symbol, Location(0, 0)),
        ]
        cursor = TokenCursor(tokens)
        stmt = SelectStatement.parse(cursor)
        assert isinstance(stmt, SelectStatement)
        assert stmt._from.value == "my_table"
        assert stmt.items[0].value == "a"
        assert stmt.items[1].value == "b"

    def test_found(self):
        tokens = [
            Token("select", Kind.keyword, Location(0, 0)),
            Token("a", Kind.identifier, Location(0, 0)),
            Token("from", Kind.keyword, Location(0, 0)),
            Token("my_table", Kind.identifier, Location(0, 0)),
            Token("where", Kind.keyword, Location(0, 0)),
            Token("x", Kind.identifier, Location(0, 0)),
            Token("=", Kind.symbol, Location(0, 0)),
            Token("hi", Kind.text, Location(0, 0)),
            Token("and", Kind.keyword, Location(0, 0)),
            Token("y", Kind.identifier, Location(0, 0)),
            Token("=", Kind.symbol, Location(0, 0)),
            Token(123, Kind.integer, Location(0, 0)),
            Token(";", Kind.symbol, Location(0, 0)),
        ]
        cursor = TokenCursor(tokens)
        stmt = SelectStatement.parse(cursor)
        assert isinstance(stmt, SelectStatement)
        assert stmt._from.value == "my_table"
        assert stmt.items[0].value == "a"

    def test_not_found(self):
        tokens = [
            Token("insert", Kind.keyword, Location(0, 0)),
            Token("into", Kind.keyword, Location(0, 0)),
            Token("users", Kind.identifier, Location(0, 0)),
            Token("values", Kind.keyword, Location(0, 0)),
            Token("(", Kind.symbol, Location(0, 0)),
            Token(1, Kind.integer, Location(0, 0)),
            Token(",", Kind.symbol, Location(0, 0)),
            Token("Phil", Kind.text, Location(0, 0)),
            Token(")", Kind.symbol, Location(0, 0)),
            Token(";", Kind.symbol, Location(0, 0)),
        ]
        cursor = TokenCursor(tokens)

        with self.assertRaises(LookupError):
            SelectStatement.parse(cursor)

        assert cursor.pointer == 0
