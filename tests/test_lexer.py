from typing import List
from toysql.lexer import StatementLexer, Kind, Token
from unittest import TestCase


class TestRepl(TestCase):
    @staticmethod
    def get_token_by_kind(tokens: List[Token], kind: Kind):
        return (token for token in tokens if token.kind == kind)

    def test_select(self):
        query = """
            select * from "my_table"
            where x = 'hi'
            and y = 123;
        """

        tokens = StatementLexer().lex(query)
        assert len(tokens) == 9

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
