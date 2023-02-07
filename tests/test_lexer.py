import unittest
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
    NullLexer,
    IdentifierLexer,
)
from unittest import TestCase
from toysql.exceptions import LexingException


class TestSymbolLexer(TestCase):
    def test_lex(self):
        lexer = SymbolLexer()
        cases = [(",b", ",", 1), ("*", "*", 1), (" *", None, 0), ("select", None, 0)]

        for source, value, pointer in cases:
            cursor = Cursor(source)
            token = lexer.lex(cursor)

            if token:
                assert cursor.pointer == pointer
                assert token.value == value
            else:
                assert cursor.pointer == pointer
                assert value is None


class TestNumericLexer(TestCase):
    def test_lex(self):
        lexer = NumericLexer()
        cases = [
            ("123 adf", 123, 3),
            ("123 ", 123, 3),
            (" 123", None, 0),
            ("1.11 ", 1.11, 4),
            ("select", None, 0),
        ]

        for source, value, pointer in cases:
            cursor = Cursor(source)
            token = lexer.lex(cursor)
            if token:
                assert token.value == value
                assert cursor.pointer == pointer
            else:
                assert value is None
                assert cursor.pointer == pointer


class TestKeywordLexer(TestCase):
    def test_lex(self):
        lexer = KeywordLexer()
        cases = [
            ("select", "select"),
            (" select", None),
            ("hello", None),
            ("from tablename", "from"),
        ]

        for source, value in cases:
            cursor = Cursor(source)
            token = lexer.lex(cursor)
            if token:
                assert token.value == value
            else:
                assert value is None


class TestLiteralLexer(TestCase):
    def test_lex(self):
        lexer = NullLexer()
        cases = [
            ("NULL", "null"),
            ("null", "null"),
            (" null", None),
            ("null tablename", "null"),
        ]

        for source, value in cases:
            cursor = Cursor(source)
            token = lexer.lex(cursor)
            if token:
                assert token.kind == Kind.null
                assert token.value == value
            else:
                assert value is None


class TestStringLexer(TestCase):
    def test_lex(self):
        lexer = StringLexer()
        cases = [("'abc'", "abc", 5), (" 'abc'", None, 0), ("select", None, 0)]

        for source, value, index in cases:
            cursor = Cursor(source)
            token = lexer.lex(cursor)
            if token:
                assert token.value == value
                assert cursor.pointer == index
            else:
                assert value is None
                assert cursor.pointer == index


class TestIdentifierLexer(TestCase):
    def test_lex(self):
        lexer = IdentifierLexer()
        cases = [
            ("my_table", "my_table"),
            ('"hello"', "hello"),
            ("12345", None),
        ]

        for source, value in cases:
            cursor = Cursor(source)
            token = lexer.lex(cursor)
            if token:
                assert token.value == value
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
        query = """select * from "my_table"\nwhere x = 'hi'\nand y = 123;"""

        tokens = self.lexer.lex(query)

        expected_tokens = [
            Token("select", Kind.keyword, Location(0, 0)),
            Token("*", Kind.symbol, Location(0, 7)),
            Token("from", Kind.keyword, Location(0, 9)),
            Token("my_table", Kind.identifier, Location(0, 14)),
            Token("where", Kind.keyword, Location(1, 0)),
            Token("x", Kind.identifier, Location(1, 6)),
            Token("=", Kind.symbol, Location(1, 8)),
            Token("hi", Kind.text, Location(1, 10)),
            Token("and", Kind.keyword, Location(2, 0)),
            Token("y", Kind.identifier, Location(2, 4)),
            Token("=", Kind.symbol, Location(2, 6)),
            Token(123, Kind.integer, Location(2, 8)),
            Token(";", Kind.symbol, Location(2, 11)),
        ]

        assert tokens == expected_tokens

    def test_select_multi_columns(self):
        query = """select x,y from "my_table"\nwhere x = 'hi'\nand y = 123;"""

        tokens = self.lexer.lex(query)

        expected_tokens = [
            Token("select", Kind.keyword, Location(0, 0)),
            Token("x", Kind.identifier, Location(0, 7)),
            Token(",", Kind.symbol, Location(0, 8)),
            Token("y", Kind.identifier, Location(0, 9)),
            Token("from", Kind.keyword, Location(0, 11)),
            Token("my_table", Kind.identifier, Location(0, 16)),
            Token("where", Kind.keyword, Location(1, 0)),
            Token("x", Kind.identifier, Location(1, 6)),
            Token("=", Kind.symbol, Location(1, 8)),
            Token("hi", Kind.text, Location(1, 10)),
            Token("and", Kind.keyword, Location(2, 0)),
            Token("y", Kind.identifier, Location(2, 4)),
            Token("=", Kind.symbol, Location(2, 6)),
            Token(123, Kind.integer, Location(2, 8)),
            Token(";", Kind.symbol, Location(2, 11)),
        ]

        assert tokens == expected_tokens

    def test_create_table(self):
        query = """CREATE TABLE u (id INT, name TEXT)"""

        tokens = self.lexer.lex(query)

        expected_tokens = [
            Token("create", Kind.keyword, Location(0, 0)),
            Token("table", Kind.keyword, Location(0, 7)),
            Token("u", Kind.identifier, Location(0, 13)),
            Token("(", Kind.symbol, Location(0, 15)),
            Token("id", Kind.identifier, Location(0, 16)),
            Token("int", Kind.keyword, Location(0, 19)),
            Token(",", Kind.symbol, Location(0, 22)),
            Token("name", Kind.identifier, Location(0, 24)),
            Token("text", Kind.keyword, Location(0, 29)),
            Token(")", Kind.symbol, Location(0, 33)),
        ]

        assert tokens == expected_tokens

    def test_insert(self):
        query = """INSERT INTO users VALUES (1, 'Phil');"""

        tokens = self.lexer.lex(query)

        expected_tokens = [
            Token("insert", Kind.keyword, Location(0, 0)),
            Token("into", Kind.keyword, Location(0, 7)),
            Token("users", Kind.identifier, Location(0, 12)),
            Token("values", Kind.keyword, Location(0, 18)),
            Token("(", Kind.symbol, Location(0, 25)),
            Token(1, Kind.integer, Location(0, 26)),
            Token(",", Kind.symbol, Location(0, 27)),
            Token("Phil", Kind.text, Location(0, 29)),
            Token(")", Kind.symbol, Location(0, 35)),
            Token(";", Kind.symbol, Location(0, 36)),
        ]

        assert tokens == expected_tokens

    def test_invalid_sql_symbol(self):
        query = """INSERT $$"""

        with self.assertRaises(LexingException) as exec_info:
            self.lexer.lex(query)

        assert str(exec_info.exception) == "Lexing error at location 0:7"


    @unittest.skip("TODO: test partial keyword")
    def test_partial_keywork(self):
        query = """CREATE TABLE schema (id INT, schema_type TEXT, name TEXT, associated_table_name TEXT, sql_text TEXT, root_page_number INT);"""

        x = self.lexer.lex(query)
        # TODO assert assert keyword as is not pulled for associated_table_name
        assert False



