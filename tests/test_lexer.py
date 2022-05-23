from typing import List
from toysql.lexer import (
    StatementLexer,
    Kind,
    Token,
    SymbolLexer,
    Cursor,
    Location,
    NumericLexer,
    StringLexer,
    KeywordLexer,
    IdentifierLexer,
)
from unittest import TestCase


class TestSymbolLexer(TestCase):
    def test_lex(self):
        lexer = SymbolLexer()
        cases = [("*", "*", 1), (" *", None, 1), ("select", None, 0)]

        for source, value, pointer in cases:
            cursor = Cursor(0, Location(0, 0))
            token, cursor = lexer.lex(source, cursor)

            if token:
                assert cursor.pointer == pointer
                assert token.value == value
            else:
                assert value is None


class TestNumericLexer(TestCase):
    def test_lex(self):
        lexer = NumericLexer()
        cases = [
            ("123", "123", 3),
            ("123 ", "123", 3),
            (" 123", None, 0),
            ("select", None, 0),
        ]

        for source, value, pointer in cases:
            cursor = Cursor(0, Location(0, 0))
            token, cursor = lexer.lex(source, cursor)
            if token:
                assert token.value == value
                assert cursor.pointer == pointer
            else:
                assert value is None


class TestStringLexer(TestCase):
    def test_lex(self):
        lexer = StringLexer()
        cases = [("'abc'", "abc"), (" 'abc'", None), ("select", None)]

        for source, value in cases:
            cursor = Cursor(0, Location(0, 0))
            token, cursor = lexer.lex(source, cursor)
            if token:
                assert token.value == value
                assert cursor.pointer == len(source)
            else:
                assert value is None


class TestKeywordLexer(TestCase):
    def test_lex(self):
        lexer = KeywordLexer()
        cases = [
            ("select", "select"),
            (" select", None),
            ("hello", None),
            ("from", "from"),
        ]

        for source, value in cases:
            cursor = Cursor(0, Location(0, 0))
            token, cursor = lexer.lex(source, cursor)
            if token:
                assert token.value == value
                assert cursor.pointer == len(value)
            else:
                assert value is None


class TestIdentifierLexer(TestCase):
    def test_lex(self):
        lexer = IdentifierLexer()
        cases = [
            ("my_table", "my_table"),
            ('"hello"', "hello"),
            ("12345", None),
        ]

        for source, value in cases:
            cursor = Cursor(0, Location(0, 0))
            token, cursor = lexer.lex(source, cursor)
            if token:
                assert token.value == value
                assert cursor.pointer == len(source)
            else:
                assert value is None


class TestStatementLexer(TestCase):
    def __init__(self, methodName) -> None:
        self.lexer = StatementLexer()
        super().__init__(methodName)

    @staticmethod
    def get_token_by_kind(tokens: List[Token], kind: Kind):
        return (token for token in tokens if token.kind == kind)

    def test_select(self):
        query = """
            select * from "my_table"
            where x = 'hi'
            and y = 123;
        """

        tokens = self.lexer.lex(query)

        # TODO cursor Location doesnt work.
        expected_tokens = [
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

        assert tokens == expected_tokens

    def test_create_table(self):
        query = """CREATE TABLE u (id INT, name TEXT)"""

        tokens = self.lexer.lex(query)

        # TODO cursor Location doesnt work.
        expected_tokens = [
            Token("create", Kind.keyword, Location(0, 0)),
            Token("table", Kind.keyword, Location(0, 0)),
            Token("u", Kind.identifier, Location(0, 0)),
            Token("(", Kind.symbol, Location(0, 0)),
            Token("id", Kind.identifier, Location(0, 0)),
            Token("int", Kind.keyword, Location(0, 0)),
            Token(",", Kind.symbol, Location(0, 0)),
            Token("name", Kind.identifier, Location(0, 0)),
            Token("text", Kind.keyword, Location(0, 0)),
            Token(")", Kind.symbol, Location(0, 0)),
        ]

        assert tokens == expected_tokens
