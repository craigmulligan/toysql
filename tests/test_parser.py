from toysql.lexer import Token, Kind, Location
from toysql.parser import SelectStatement
from unittest import TestCase


class TestSelectParser(TestCase):
    def test_found(self):
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
        cursor = 0

        stmt, cursor = SelectStatement.parse(tokens, cursor)
        assert isinstance(stmt, SelectStatement)
        assert stmt._from.value == "my_table"

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
        cursor = 0
        stmt, cursor = SelectStatement.parse(tokens, cursor)
        assert not stmt
        assert cursor == 0
