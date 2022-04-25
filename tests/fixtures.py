from unittest import TestCase
from toysql.vm import VM
import os


class Fixtures(TestCase):
    db_file_path: str

    def setUp(self) -> None:
        self.db_file_path = "__testdb__.db"
        if os.path.exists(self.db_file_path):
            os.remove(self.db_file_path)
        self.vm = VM(self.db_file_path)
        self.pager = self.vm.pager
        self.table = self.vm.table

        return super().setUp()
