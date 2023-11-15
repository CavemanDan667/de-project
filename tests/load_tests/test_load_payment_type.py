from src.loading.load_utils.load_payment_type import (
    load_payment_type
)
from src.loading.load_utils.get_credentials import get_credentials
from pg8000.native import Connection, DatabaseError
from dotenv import dotenv_values
import pytest
import subprocess
from unittest.mock import MagicMock

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


@pytest.fixture()
def conn():
    return Connection(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database)


def test_function_returns_success_message(conn):
    result = load_payment_type(
       's3://de-project-test-data/parquet/test-payment-type.parquet',
       conn
    )
    assert result == 'Data loaded successfully - dim_payment_type'


def test_function_correctly_populates_table(conn):
    load_payment_type(
       's3://de-project-test-data/parquet/test-payment-type.parquet',
       conn
    )
    load_payment_type(
       's3://de-project-test-data/parquet/test-payment-type-plus.parquet',
       conn
    )
    result = conn.run('SELECT * FROM dim_payment_type;')
    assert result[0] == [1, 'TYPE_ONE']
    assert result[1] == [2, 'TYPE_TWO']
    assert result[2] == [3, 'TYPE_THREE']
    assert result[3] == [4, 'TYPE_FOUR']
    assert result[4] == [5, 'TYPE_FIVE']


def test_function_does_not_duplicate_data(conn):
    load_payment_type(
       's3://de-project-test-data/parquet/test-payment-type.parquet',
       conn
    )
    load_payment_type(
       's3://de-project-test-data/parquet/test-payment-type.parquet',
       conn
    )
    load_payment_type(
       's3://de-project-test-data/parquet/test-payment-type-plus.parquet',
       conn
    )
    load_payment_type(
       's3://de-project-test-data/parquet/test-payment-type-plus.parquet',
       conn
    )
    load_payment_type(
       's3://de-project-test-data/parquet/test-payment-type-plus.parquet',
       conn
    )
    result = conn.run('SELECT * FROM dim_payment_type;')
    assert len(result) == 5


def test_function_returns_key_error_with_incorrect_data(conn):
    with pytest.raises(KeyError):
        load_payment_type(
            's3://de-project-test-data/parquet/test-currency.parquet',
            conn
        )


def test_function_calls_conn_with_correct_SQL_query():
    mock_conn = MagicMock()
    load_payment_type(
        "s3://de-project-test-data/parquet/test-payment-type.parquet",
        mock_conn
        )
    expected_insert_query_list = [
        "INSERT INTO dim_payment_type",
        "(payment_type_id, payment_type_name)",
        "VALUES",
        ]
    assert mock_conn.run.call_count == 8
    assert expected_insert_query_list[0] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[1] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[2] in str(mock_conn.run.call_args)


def test_function_handles_DatabaseError(caplog):
    mock_conn = MagicMock()
    mock_conn.run.side_effect = DatabaseError()

    with pytest.raises(DatabaseError):
        load_payment_type(
            's3://de-project-test-data/parquet/test-payment-type.parquet',
            mock_conn)
    assert 'load_payment_type has raised an error' in caplog.text


def test_function_handles_IndexError(caplog):
    mock_conn = MagicMock()
    mock_conn.run.side_effect = IndexError()

    with pytest.raises(IndexError):
        load_payment_type(
            's3://de-project-test-data/parquet/test-payment-type.parquet',
            mock_conn)
    assert 'load_payment_type has raised an error' in caplog.text
