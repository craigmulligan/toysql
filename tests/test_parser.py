from toysql.lexer import Token, Kind, Location
from toysql.parser import SelectStatement, TokenCursor
from unittest import TestCase


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
            Token("hi", Kind.string, Location(0, 0)),
            Token("and", Kind.keyword, Location(0, 0)),
            Token("y", Kind.identifier, Location(0, 0)),
            Token("=", Kind.symbol, Location(0, 0)),
            Token("123", Kind.numeric, Location(0, 0)),
            Token(";", Kind.symbol, Location(0, 0)),
        ]
        cursor = TokenCursor(tokens)
        stmt = SelectStatement.parse(cursor)
        assert isinstance(stmt, SelectStatement)
        assert stmt._from.value == "my_table"
        assert stmt.items[0].value == "*"

    def test_found(self):
        tokens = [
            Token("select", Kind.keyword, Location(0, 0)),
            Token("a", Kind.identifier, Location(0, 0)),
            Token("from", Kind.keyword, Location(0, 0)),
            Token("my_table", Kind.identifier, Location(0, 0)),
            Token("where", Kind.keyword, Location(0, 0)),
            Token("x", Kind.identifier, Location(0, 0)),
            Token("=", Kind.symbol, Location(0, 0)),
            Token("hi", Kind.string, Location(0, 0)),
            Token("and", Kind.keyword, Location(0, 0)),
            Token("y", Kind.identifier, Location(0, 0)),
            Token("=", Kind.symbol, Location(0, 0)),
            Token("123", Kind.numeric, Location(0, 0)),
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
            Token("1", Kind.numeric, Location(0, 0)),
            Token(",", Kind.symbol, Location(0, 0)),
            Token("Phil", Kind.string, Location(0, 0)),
            Token(")", Kind.symbol, Location(0, 0)),
            Token(";", Kind.symbol, Location(0, 0)),
        ]
        cursor = TokenCursor(tokens)

        with self.assertRaises(LookupError):
            SelectStatement.parse(cursor)

        assert cursor.pointer == 0
