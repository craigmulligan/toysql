import pytest
import os
from toysql.vm import VM
from toysql.table import Table


@pytest.fixture(scope="function")
def db_file_path():
    db_file_path = "__testdb__.db"
    if os.path.exists(db_file_path):
        os.remove(db_file_path)
    return db_file_path


@pytest.fixture()
def vm(db_file_path):
    virtual_machine = VM(db_file_path)
    return virtual_machine


@pytest.fixture()
def table(db_file_path):
    table = Table(db_file_path)
    return table
