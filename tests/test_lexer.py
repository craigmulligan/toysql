from toysql.lexer import StatementLexer, Kind
from unittest import TestCase


class TestRepl(TestCase):
    def test_select(self):
        query = """
            select * from my_table 
            where x = 'hi';
        """
        tokens = StatementLexer().lex(query)
        assert len(tokens) == 1
        string_token = [token for token in tokens if token.kind == Kind.string][0]
        assert string_token.value == "hi"

        # Once we have all lexers the cursor will be correct.
        # assert string_token.loc.col == 34
        # assert string_token.loc.line == 0
