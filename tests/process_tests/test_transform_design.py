from src.process.process_utils.transform_design import transform_design
from pg8000.native import Connection
import pytest
import pandas as pd
from tests.get_credentials import get_credentials

config = get_credentials('test_dw_creds')

user = config["TESTDW_USER"]
password = config["TESTDW_PASSWORD"]
host = config["TESTDW_HOST"]
port = config["TESTDW_PORT"]
database = config["TESTDW_DATABASE"]


@pytest.fixture()
def conn():
    return Connection(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database)


def test_function_returns_data_frame(conn):
    result = transform_design(
        'tests/csv_test_files/test-design.csv',
        conn
        )
    assert isinstance(result, pd.core.frame.DataFrame)


def test_function_returns_correct_data(conn):
    result = transform_design(
       'tests/csv_test_files/test-design.csv',
       conn
    )
    assert result.values.tolist() == [
        [18, 'Name1', '/usr', 'name1-20000101-abcd.json'],
        [29, 'Name2', '/private', 'name2-20000101-4eff.json'],
        [345, 'Name3', '/private/var', 'name3-20000101-3ghj.json'],
        [4, 'Name3', '/private/var', 'name3-20000101-klmn.json'],
        [52, 'Name2', '/lost+found', 'name2-20000101-p123.json']
    ]


def test_function_correctly_populates_table(conn):
    transform_design(
       'tests/csv_test_files/test-design.csv',
       conn
    )
    result = conn.run('SELECT * FROM dim_design;')
    assert result[0] == [18, 'Name1', '/usr', 'name1-20000101-abcd.json']
    assert result[1] == [29, 'Name2', '/private', 'name2-20000101-4eff.json']
    assert result[2] == [
        345,
        'Name3',
        '/private/var',
        'name3-20000101-3ghj.json'
    ]
    assert result[3] == [
        4,
        'Name3',
        '/private/var',
        'name3-20000101-klmn.json'
    ]
    assert result[4] == [
        52,
        'Name2',
        '/lost+found',
        'name2-20000101-p123.json'
    ]


def test_function_does_not_repeat_duplicate_data(conn):
    transform_design(
       'tests/csv_test_files/test-design.csv',
       conn
    )
    transform_design(
       'tests/csv_test_files/test-design.csv',
       conn
    )
    result = conn.run('SELECT * FROM dim_design;')
    assert len(result) == 5


def test_function_correctly_updates_data(conn):
    result = transform_design(
       'tests/csv_test_files/test-design-update.csv',
       conn
    )
    assert result.values.tolist() == [[
        52,
        'NewName2',
        '/lost+found',
        'newname2-20000101-p123.json'
    ]]
    query = 'SELECT * FROM dim_design;'
    result_data = conn.run(query)
    assert result_data == [
        [18, 'Name1', '/usr', 'name1-20000101-abcd.json'],
        [29, 'Name2', '/private', 'name2-20000101-4eff.json'],
        [345, 'Name3', '/private/var', 'name3-20000101-3ghj.json'],
        [4, 'Name3', '/private/var', 'name3-20000101-klmn.json'],
        [52, 'NewName2', '/lost+found', 'newname2-20000101-p123.json']
    ]
