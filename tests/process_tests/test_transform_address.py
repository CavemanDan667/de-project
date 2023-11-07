from src.process.process_utils.transform_address import transform_address
from pg8000.native import Connection
from dotenv import dotenv_values
import pytest
import pandas as pd

config = dotenv_values(".env")

user = config["TESTDW_USER"]
password = config["TESTDW_PASSWORD"]
host = config["TESTDW_HOST"]
port = config["TESTDW_PORT"]
database = config["TESTDW_DATABASE"]


@pytest.fixture
def conn():
    return Connection(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database)


def test_function_returns_data_frame(conn):
    result = transform_address(
       'tests/csv_test_files/test-address.csv',
       conn
    )
    assert isinstance(result, pd.core.frame.DataFrame)


def test_function_returns_correct_data(conn):
    result = transform_address(
       'tests/csv_test_files/test-address.csv',
       conn
    )
    assert result.values.tolist()[3] == [
        4,
        '5 Far Lane',
        'Parkway',
        'North Shore',
        'Castletown',
        'AB2 3CD',
        'Wales',
        '1234 800900'
    ]


def test_function_inserts_correct_data(conn):
    transform_address(
        'tests/csv_test_files/test-address.csv',
        conn
    )
    select_query = '''SELECT * FROM dim_location
    WHERE location_id = 4;'''
    result = conn.run(select_query)
    assert result == [[
        4,
        '5 Far Lane',
        'Parkway',
        'North Shore',
        'Castletown',
        'AB2 3CD',
        'Wales',
        '1234 800900'
    ]]


def test_function_replaces_empty_values_with_none(conn):
    transform_address(
       'tests/csv_test_files/test-address.csv',
       conn
    )
    select_query = '''SELECT * FROM dim_location
    WHERE location_id = 2;'''
    result = conn.run(select_query)
    assert result == [[
        2,
        "234 St. Steven's Road",
        None,
        None,
        'Old Town',
        '22222-3333',
        'Northern Ireland',
        '07700 100200'
    ]]


def test_function_does_not_duplicate_data(conn):
    transform_address(
        'tests/csv_test_files/test-address.csv',
        conn
    )
    transform_address(
        'tests/csv_test_files/test-address.csv',
        conn
    )
    result = conn.run(
        'SELECT * FROM dim_location;'
    )
    assert len(result) == 5
