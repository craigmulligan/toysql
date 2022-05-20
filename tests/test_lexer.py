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
        cases = [("*", "*"), (" *", None), ("select", None)]

        for source, value in cases:
            cursor = Cursor(0, Location(0, 0))
            token, cursor = lexer.lex(source, cursor)

            if token:
                assert cursor.pointer == len(source)
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
    @staticmethod
    def get_token_by_kind(tokens: List[Token], kind: Kind):
        return (token for token in tokens if token.kind == kind)

    # @skip("oops")
    def test_select(self):
        query = """
            select * from "my_table"
            where x = 'hi'
            and y = 123;
        """

        tokens = StatementLexer().lex(query)

        assert len(tokens) == 13

        keyword_tokens = self.get_token_by_kind(tokens, Kind.keyword)
        expected_keywords = ["select", "from", "where", "and"]

        for keyword in expected_keywords:
            assert next(keyword_tokens).value == keyword

        symbol_tokens = self.get_token_by_kind(tokens, Kind.symbol)
        expected_symbols = ["*", "=", "=", ";"]
        for keyword in expected_symbols:
            assert next(symbol_tokens).value == keyword

        numeric_tokens = self.get_token_by_kind(tokens, Kind.numeric)
        assert next(numeric_tokens).value == "123"

        string_tokens = self.get_token_by_kind(tokens, Kind.string)
        assert next(string_tokens).value == "hi"

        identifier_tokens = self.get_token_by_kind(tokens, Kind.identifier)
        expected_identifiers = ["my_table", "x", "y"]

        for identifier in expected_identifiers:
            assert next(identifier_tokens).value == identifier
