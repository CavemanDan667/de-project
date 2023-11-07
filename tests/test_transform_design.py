from src.process.transform_design import transform_design
from pg8000.native import Connection, DatabaseError
from dotenv import dotenv_values
import pytest


config = dotenv_values(".env")


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
    assert result.values.tolist() == [
        ['Name1', '/usr', 'name1-20000101-abcd.json'],
        ['Name2', '/private', 'name2-20000101-4eff.json'],
        ['Name3', '/private/var', 'name3-20000101-3ghj.json'],
        ['Name3', '/private/var', 'name3-20000101-klmn.json'],
        ['Name2', '/lost+found', 'name2-20000101-p123.json']
    ]


def test_function_raises_database_error_if_query_fails():
    conn = Connection(
        user=config["TEST_USER"],
        password=config["TEST_PASSWORD"],
        host=config["TEST_HOST"],
        port=config["TEST_PORT"],
        database=config["TEST_DATABASE"]
    )
    with pytest.raises(DatabaseError):
        transform_design(
            'tests/csv_test_files/test-design.csv',
            conn
        )
