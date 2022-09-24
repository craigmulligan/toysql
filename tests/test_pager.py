from snapshottest import TestCase
from toysql.pager import Pager
from tests.fixtures import Fixtures


class TestPager(Fixtures, TestCase):
    def test_is_corrupt(self):
        """If the file is not n * page size it should fail."""
        assert not Pager(self.db_file_path).is_corrupt()

        with open(self.db_file_path, "rb+") as f:
            f.seek(0, 2)  # move to end
            f.write(str.encode("some extra bytes"))
            f.flush()

        with self.assertRaises(Exception):
            Pager(self.db_file_path)
