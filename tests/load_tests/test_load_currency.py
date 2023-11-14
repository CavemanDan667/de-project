from src.loading.load_utils.load_currency import load_currency
from pg8000.native import Connection, DatabaseError
import pytest
from src.loading.load_utils.get_credentials import get_credentials
import subprocess
from dotenv import dotenv_values

identity = subprocess.check_output('whoami')

if identity == b'runner\n':
    config = get_credentials('test_dw_creds')
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
        's3://de-project-test-data/parquet/test-currency.parquet',
        conn
        )
    assert result == 'Data loaded successfully - dim_currency'


def test_function_correctly_populates_table(conn):
    load_currency(
       's3://de-project-test-data/parquet/test-currency.parquet',
       conn
    )
    result = conn.run('SELECT * FROM dim_currency;')
    assert result[0] == [1, 'GBP', 'British pound']
    assert result[1] == [2, 'USD', 'United States dollar']
    assert result[2] == [3, 'EUR', 'European Euro']


def test_function_does_not_repeat_duplicate_data(conn):
    load_currency(
       's3://de-project-test-data/parquet/test-currency.parquet',
       conn
    )
    load_currency(
       's3://de-project-test-data/parquet/test-currency.parquet',
       conn
    )
    result = conn.run('SELECT * FROM dim_currency;')
    assert len(result) == 3


def test_function_raises_error_on_null_data(conn):
    with pytest.raises(DatabaseError):
        load_currency(
            's3://de-project-test-data/parquet/test-fake-currency',
            conn
        )


def test_function_raises_error_with_incorrect_parquet_file(conn):
    with pytest.raises(KeyError):
        load_currency(
            's3://de-project-test-data/parquet/test-payment-type.parquet',
            conn
        )
