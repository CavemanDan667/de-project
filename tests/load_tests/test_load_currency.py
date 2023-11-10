from src.loading.load_utils.load_currency import load_currency
from pg8000.native import Connection, DatabaseError
import pytest
from tests.get_credentials import get_credentials
import subprocess
from dotenv import dotenv_values

identity = subprocess.check_output('whoami')

if identity == b'runner\n':
    config = get_credentials('test_totesys_db_creds')
else:
    config = dotenv_values('.env')

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


def test_function_returns_success_message(conn):
    result = load_currency(
        'tests/parquet_test_files/test-currency',
        conn
        )
    assert result == 'Data loaded successfully - dim_currency'


def test_function_correctly_populates_table(conn):
    load_currency(
       'tests/parquet_test_files/test-currency',
       conn
    )
    result = conn.run('SELECT * FROM dim_currency;')
    assert result[0] == [1, 'GBP', 'British pound']
    assert result[1] == [2, 'USD', 'United States dollar']
    assert result[2] == [3, 'EUR', 'European Euro']


def test_function_does_not_repeat_duplicate_data(conn):
    load_currency(
       'tests/parquet_test_files/test-currency',
       conn
    )
    load_currency(
       'tests/parquet_test_files/test-currency',
       conn
    )
    result = conn.run('SELECT * FROM dim_currency;')
    assert len(result) == 3


def test_function_raises_error_on_null_data(conn):
    with pytest.raises(DatabaseError):
        load_currency('tests/parquet_test_files/test-fake-currency', conn)
