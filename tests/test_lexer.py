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
from unittest import TestCase, skip


class TestSymbolLexer(TestCase):
    def test_lex(self):
        lexer = SymbolLexer()
        cases = [("*", True, 1), (" *", False, 1), ("select", False, 0)]

        for source, found, pointer in cases:
            cursor = Cursor(0, Location(0, 0))
            tokens, cursor = lexer.lex(source, cursor)
            assert cursor.pointer == pointer
            assert bool(tokens) is found


class TestNumericLexer(TestCase):
    def test_lex(self):
        lexer = NumericLexer()
        cases = [("123", True, 4), (" 123", False, 0), ("select", False, 0)]

        for source, found, pointer in cases:
            cursor = Cursor(0, Location(0, 0))
            tokens, cursor = lexer.lex(source, cursor)
            assert cursor.pointer == pointer
            assert bool(tokens) is found


class TestStringLexer(TestCase):
    def test_lex(self):
        lexer = StringLexer()
        cases = [("'abc'", "abc", 5), (" 'abc'", None, 0), ("select", None, 0)]

        for source, value, pointer in cases:
            cursor = Cursor(0, Location(0, 0))
            token, cursor = lexer.lex(source, cursor)
            if token:
                assert token.value == value
            else:
                assert value is None

            assert cursor.pointer == pointer


class TestKeywordLexer(TestCase):
    def test_lex(self):
        lexer = KeywordLexer()
        cases = [("select", "select", 6), (" select", None, 0), ("hello", None, 0)]

        for source, value, pointer in cases:
            cursor = Cursor(0, Location(0, 0))
            token, cursor = lexer.lex(source, cursor)
            if token:
                assert token.value == value
            else:
                assert value is None

            assert cursor.pointer == pointer


class TestIdentifierLexer(TestCase):
    def test_lex(self):
        lexer = IdentifierLexer()
        cases = [
            ("my_table", "my_table", 8),
            ('"hello"', "hello", 7),
            ("12345", None, 0),
        ]

        for source, value, pointer in cases:
            cursor = Cursor(0, Location(0, 0))
            token, cursor = lexer.lex(source, cursor)
            if token:
                assert token.value == value
            else:
                assert value is None

            assert cursor.pointer == pointer


class TestStatementLexer(TestCase):
    @staticmethod
    def get_token_by_kind(tokens: List[Token], kind: Kind):
        return (token for token in tokens if token.kind == kind)

    @skip("todo")
    def test_select(self):
        query = """
            select * from "my_table"
            where x = 'hi'
            and y = 123;
        """

        tokens = StatementLexer().lex(query)

        assert len(tokens) == 11

        numeric_tokens = self.get_token_by_kind(tokens, Kind.numeric)
        assert next(numeric_tokens).value == "123"

        identifier_tokens = self.get_token_by_kind(tokens, Kind.identifier)
        expected_identifiers = ["my_table", "x", "y"]

        for identifier in expected_identifiers:
            assert next(identifier_tokens).value == identifier

        string_tokens = self.get_token_by_kind(tokens, Kind.string)
        assert next(string_tokens).value == "hi"

        keyword_tokens = self.get_token_by_kind(tokens, Kind.keyword)
        expected_keywords = ["select", "from", "where", "and"]

        for keyword in expected_keywords:
            assert next(keyword_tokens).value == keyword

        symbol_tokens = self.get_token_by_kind(tokens, Kind.symbol)
        expected_symbols = ["*", ";"]
        for keyword in expected_symbols:
            assert next(symbol_tokens).value == keyword
