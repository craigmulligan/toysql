from typing import List
from toysql.lexer import (
    Kind,
    Keyword,
    Identifier,
    DataType,
    Symbol,
    Token,
    Cursor,
    Location,
    symbol_lexer,
    numeric_lexer,
    text_lexer,
    keyword_lexer,
    identifier_lexer,
    lex,
)
from unittest import TestCase
from toysql.exceptions import LexingException


class TestSymbolLexer(TestCase):
    def test_lex(self):
        cases = [(",b", ",", 1), ("*", "*", 1), (" *", None, 0), ("select", None, 0)]

        for source, value, pointer in cases:
            cursor = Cursor(source)
            token = symbol_lexer(cursor)

            assert cursor.pointer == pointer
            if token:
                assert token.value == value
            else:
                assert value is None


class TestNumericLexer(TestCase):
    def test_lex(self):
        cases = [
            ("123 adf", "123", 3),
            ("123 ", "123", 3),
            (" 123", None, 0),
            ("1.11 ", "1.11", 4),
            ("select", None, 0),
        ]

        for source, value, pointer in cases:
            cursor = Cursor(source)
            token = numeric_lexer(cursor)
            if token:
                assert token.value == value
                assert cursor.pointer == pointer
            else:
                assert value is None
                assert cursor.pointer == pointer


class TestKeywordLexer(TestCase):
    def test_lex(self):
        cases = [
            ("select", "select"),
            (" select", None),
            ("hello", None),
            ("from tablename", "from"),
        ]

        for source, value in cases:
            cursor = Cursor(source)
            token = keyword_lexer(cursor)

            if token:
                assert token.value == value
            else:
                assert value is None


class TestStringLexer(TestCase):
    def test_lex(self):
        cases = [("'abc'", "abc", 5), (" 'abc'", None, 0), ("select", None, 0)]

        for source, value, index in cases:
            cursor = Cursor(source)
            token = text_lexer(cursor)
            if token:
                assert token.value == value
                assert cursor.pointer == index
            else:
                assert value is None
                assert cursor.pointer == index


class TestIdentifierLexer(TestCase):
    def test_lex(self):
        cases = [
            ("my_table", "my_table"),
            ('"hello"', "hello"),
            ("12345", None),
        ]

        for source, value in cases:
            cursor = Cursor(source)
            token = identifier_lexer(cursor)
            if token:
                assert token.value == value
            else:
                assert value is None


class TestStatementLexer(TestCase):
    @staticmethod
    def get_token_by_kind(tokens: List[Token], kind: Kind):
        return (token for token in tokens if token.kind == kind)

    def test_select_x(self):
        query = """select * from "my_table"\nwhere x = 'hi'\nand y = 123;"""

        tokens = lex(query)

        expected_tokens = [
            Token(Keyword.select, loc=Location(0, 0)),
            Token(Symbol.asterisk, loc=Location(0, 7)),
            Token(Keyword._from, loc=Location(0, 9)),
            Token(Identifier.long, value="my_table", loc=Location(0, 14)),
            Token(Keyword.where, loc=Location(1, 0)),
            Token(Identifier.long, value="x", loc=Location(1, 6)),
            Token(Symbol.equal, loc=Location(1, 8)),
            Token(DataType.text, value="hi", loc=Location(1, 10)),
            Token(Keyword._and, loc=Location(2, 0)),
            Token(Identifier.long, value="y", loc=Location(2, 4)),
            Token(Symbol.equal, loc=Location(2, 6)),
            Token(DataType.integer, value="123", loc=Location(2, 8)),
            Token(Symbol.semicolon, loc=Location(2, 11)),
        ]

        assert tokens == expected_tokens

    def test_select_multi_columns(self):
        query = """select x,y from "my_table"\nwhere x = 'hi'\nand y = 123;"""

        tokens = lex(query)

        expected_tokens = [
            Token(Keyword.select, loc=Location(0, 0)),
            Token(Identifier.long, value="x", loc=Location(0, 7)),
            Token(Symbol.comma, loc=Location(0, 8)),
            Token(Identifier.long, value="y", loc=Location(0, 9)),
            Token(Keyword._from, loc=Location(0, 11)),
            Token(Identifier.long, value="my_table", loc=Location(0, 16)),
            Token(Keyword.where, loc=Location(1, 0)),
            Token(Identifier.long, value="x", loc=Location(1, 6)),
            Token(Symbol.equal, loc=Location(1, 8)),
            Token(DataType.text, value="hi", loc=Location(1, 10)),
            Token(Keyword._and, loc=Location(2, 0)),
            Token(Identifier.long, value="y", loc=Location(2, 4)),
            Token(Symbol.equal, loc=Location(2, 6)),
            Token(DataType.integer, value="123", loc=Location(2, 8)),
            Token(Symbol.semicolon, loc=Location(2, 11)),
        ]

        assert tokens == expected_tokens

    def test_create_table(self):
        query = """CREATE TABLE u (id INTEGER, name TEXT)"""

        tokens = lex(query)

        expected_tokens = [
            Token(Keyword.create, loc=Location(0, 0)),
            Token(Keyword.table, loc=Location(0, 7)),
            Token(Identifier.long, value="u", loc=Location(0, 13)),
            Token(Symbol.left_paren, loc=Location(0, 15)),
            Token(Identifier.long, value="id", loc=Location(0, 16)),
            Token(Keyword.integer, loc=Location(0, 19)),
            Token(Symbol.comma, loc=Location(0, 26)),
            Token(Identifier.long, value="name", loc=Location(0, 28)),
            Token(Keyword.text, loc=Location(0, 33)),
            Token(Symbol.right_paren, loc=Location(0, 37)),
        ]

        assert tokens == expected_tokens

    def test_create_table_with_pk(self):
        query = """CREATE TABLE u (id INTEGER PRIMARY KEY, name TEXT)"""

        tokens = lex(query)

        expected_tokens = [
            Token(Keyword.create, loc=Location(0, 0)),
            Token(Keyword.table, loc=Location(0, 7)),
            Token(Identifier.long, value="u", loc=Location(0, 13)),
            Token(Symbol.left_paren, loc=Location(0, 15)),
            Token(Identifier.long, value="id", loc=Location(0, 16)),
            Token(Keyword.integer, loc=Location(0, 19)),
            Token(Keyword.primary, loc=Location(0, 27)),
            Token(Keyword.key, loc=Location(0, 35)),
            Token(Symbol.comma, loc=Location(0, 38)),
            Token(Identifier.long, value="name", loc=Location(0, 40)),
            Token(Keyword.text, loc=Location(0, 45)),
            Token(Symbol.right_paren, loc=Location(0, 49)),
        ]

        assert tokens == expected_tokens

    def test_insert(self):
        query = """INSERT INTO users VALUES (1, 'Phil');"""

        tokens = lex(query)

        expected_tokens = [
            Token(Keyword.insert, loc=Location(0, 0)),
            Token(Keyword.into, loc=Location(0, 7)),
            Token(Identifier.long, value="users", loc=Location(0, 12)),
            Token(Keyword.values, loc=Location(0, 18)),
            Token(Symbol.left_paren, loc=Location(0, 25)),
            Token(DataType.integer, value="1", loc=Location(0, 26)),
            Token(Symbol.comma, loc=Location(0, 27)),
            Token(DataType.text, value="Phil", loc=Location(0, 29)),
            Token(Symbol.right_paren, loc=Location(0, 35)),
            Token(Symbol.semicolon, loc=Location(0, 36)),
        ]

        assert tokens == expected_tokens

    def test_invalid_sql_symbol(self):
        query = """INSERT $$"""

        with self.assertRaises(LexingException) as exec_info:
            lex(query)

        assert str(exec_info.exception) == "Lexing error at location 0:7"

    def test_partial_keywork(self):
        query = """CREATE TABLE schema (id INTEGER, schema_type TEXT, name TEXT, associated_table_name TEXT, sql_text TEXT, root_page_number INTEGER);"""
        tokens = lex(query)

        assert Token(Keyword._as, loc=Location(line=0, col=58)) not in tokens
