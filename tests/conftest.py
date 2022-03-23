import pytest
from toysql.vm import VM


@pytest.fixture()
def vm():
    virtual_machine = VM()
    return virtual_machine
