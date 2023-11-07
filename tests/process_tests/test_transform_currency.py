from src.process.transform_currency import transform_currency
from pg8000.native import Connection, DatabaseError
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
    result = transform_currency(
        'tests/csv_test_files/test-currency.csv',
        conn
        )
    assert isinstance(result, pd.core.frame.DataFrame)


def test_function_returns_correct_data(conn):
    result = transform_currency(
       'tests/csv_test_files/test-currency.csv',
       conn
    )
    assert result.values.tolist() == [
        ['GBP', 'British pound'],
        ['USD', 'United States dollar'],
        ['EUR', 'European Euro']
    ]


def test_function_correctly_populates_table(conn):
    transform_currency(
       'tests/csv_test_files/test-currency.csv',
       conn
    )
    result = conn.run('SELECT * FROM dim_currency;')
    assert result[0] == [1, 'GBP', 'British pound']
    assert result[1] == [2, 'USD', 'United States dollar']
    assert result[2] == [3, 'EUR', 'European Euro']


def test_function_does_not_repeat_duplicate_data(conn):
    transform_currency(
       'tests/csv_test_files/test-currency.csv',
       conn
    )
    transform_currency(
       'tests/csv_test_files/test-currency.csv',
       conn
    )
    result = conn.run('SELECT * FROM dim_currency;')
    assert len(result) == 3


def test_function_raises_database_error_if_query_fails():
    conn = Connection(
        user=config["TEST_USER"],
        password=config["TEST_PASSWORD"],
        host=config["TEST_HOST"],
        port=config["TEST_PORT"],
        database=config["TEST_DATABASE"]
    )
    with pytest.raises(DatabaseError):
        transform_currency(
            'tests/csv_test_files/test-currency.csv',
            conn
        )
