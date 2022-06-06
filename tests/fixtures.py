from unittest import TestCase
from toysql.vm import VM
from toysql.table import Table
import toysql.datatypes as datatypes
from toysql.constants import USERNAME_SIZE, EMAIL_SIZE
import tempfile


class Fixtures(TestCase):
    db_file_path: str

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_file_path = self.temp_dir.name + "/__testdb__.db"

        self.vm = VM(self.db_file_path)
        self.pager = self.vm.pager
        self.table = Table(
            self.pager,
            {
                "id": datatypes.Integer(),
                "name": datatypes.String(USERNAME_SIZE),
                "email": datatypes.String(EMAIL_SIZE),
            },
        )

        return super().setUp()

    def cleanUp(self) -> None:
        self.temp_dir.cleanup()
