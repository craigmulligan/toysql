from unittest import TestCase
from toysql.vm import VM
import tempfile


class Fixtures(TestCase):
    db_file_path: str

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_file_path = self.temp_dir.name + "/__testdb__.db"

        self.vm = VM(self.db_file_path)
        self.pager = self.vm.pager
        return super().setUp()

    def cleanUp(self) -> None:
        self.temp_dir.cleanup()
