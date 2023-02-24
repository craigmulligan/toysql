from unittest import mock, TestCase
from toysql.pager import Pager
from toysql.vm import VM
from toysql.compiler import Compiler
from os import path


class TestFmt(TestCase):
    def test_success(self):
        "CREATE TABLE courses(code INTEGER PRIMARY KEY, name TEXT, prof BYTE, dept INTEGER);"
        p = path.join("tests", "files", "databases", "1table-1page.cdb")
        pager = Pager(p)
        vm = VM(pager)
        compiler = Compiler(pager)
        program = compiler.compile(f"SELECT * FROM courses")

        [x for x in vm.execute(program)]

        # // TODO assert results
        # 21000  "Programming Languages"   75    89
        # 23500  "Databases"               NULL  42
        # 27500  "Operating Systems"       NULL  89
