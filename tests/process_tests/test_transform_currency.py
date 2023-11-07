from src.process.transform_currency import transform_currency
from pg8000.native import Connection, DatabaseError

from dotenv import dotenv_values
import pytest

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
    assert result.values.tolist() == [
        ['GBP', 'British pound'],
        ['USD', 'United States dollar'],
        ['EUR', 'European Euro']
    ]


def test_function_raises_database_error_if_query_fails():
    conn = Connection(
        user='user',
        password='password',
        host='host',
        port='port',
        database='database'
    )
    with pytest.raises(DatabaseError):
        transform_currency(
            'tests/csv_test_files/test-currency.csv',
            conn
        )
