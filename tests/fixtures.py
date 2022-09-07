from unittest import TestCase
from toysql.pager import Pager
import tempfile


class Fixtures(TestCase):
    db_file_path: str

    def setUp(self) -> None:
        super().setUp()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_file_path = self.temp_dir.name + "/__testdb__.db"
        self.pager = Pager(self.db_file_path)

    def cleanUp(self) -> None:
        self.temp_dir.cleanup()
