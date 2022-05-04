import logging
from toysql.repl import repl
from unittest import mock, TestCase


class TestRepl(TestCase):
    def test_success(self):
        mock_callback = mock.Mock()
        query = "select * from my_table;"

        with self.assertLogs(level=logging.INFO) as caplog, mock.patch(
            "builtins.input", lambda _: query
        ):
            repl(mock_callback)
            if caplog:
                assert f"your query was {query}" in caplog.output[0]
            else:
                raise Exception("Logs not raised")

        mock_callback.assert_called_once()

    def test_failure(self):
        mock_callback = mock.Mock()
        query = "selct * from my_table;"

        with self.assertLogs(level=logging.ERROR) as caplog, mock.patch(
            "builtins.input", lambda _: query
        ):
            repl(mock_callback)
            if caplog:
                assert f"Oops something went wrong: Invalid query" in caplog.output[0]
            else:
                raise Exception("Logs not raised")

        mock_callback.assert_called_once()
