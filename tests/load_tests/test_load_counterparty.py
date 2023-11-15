from src.loading.load_utils.load_counterparty import load_counterparty
from src.loading.load_utils.get_credentials import get_credentials
from src.loading.load_utils.load_address import (
    load_address
)
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
    load_address(
        "s3://de-project-test-data/parquet/test-address.parquet",
        conn
    )
    result = load_counterparty(
       's3://de-project-test-data/parquet/test-counterparty.parquet',
       conn
    )
    assert result == 'Data loaded successfully - dim_counterparty'


def test_function_inserts_data_into_table(conn):
    load_address(
        "s3://de-project-test-data/parquet/test-address.parquet",
        conn
    )
    load_counterparty(
       's3://de-project-test-data/parquet/test-counterparty.parquet',
       conn
    )
    result = conn.run(
        'SELECT * FROM dim_counterparty WHERE counterparty_id < 3;'
    )
    assert result == [[1, 'Company and Sons', "234 St. Steven's Road",
                       None, None, 'Old Town', '22222-3333',
                       'Northern Ireland', '07700 100200'],
                      [2, 'Clarke, Hunter and Lorimer', '123 Main Street',
                       None, 'Central', 'New Town', '12345',
                       'England', '1234 567890']]


def test_function_does_not_duplicate_data(conn):
    load_address(
        "s3://de-project-test-data/parquet/test-address.parquet",
        conn
    )
    result = load_counterparty(
       's3://de-project-test-data/parquet/test-counterparty.parquet',
       conn
    )
    result = load_counterparty(
       's3://de-project-test-data/parquet/test-counterparty.parquet',
       conn
    )
    result = conn.run('SELECT * FROM dim_counterparty;')
    assert len(result) == 4


def test_function_can_update_data(conn):
    load_address(
        "s3://de-project-test-data/parquet/test-address.parquet",
        conn
    )
    result = load_counterparty(
       's3://de-project-test-data/parquet/test-counterparty-update.parquet',
       conn
    )
    result = conn.run(
        'SELECT * FROM dim_counterparty WHERE counterparty_id = 4;'
    )
    assert result[0] == [
        4, 'Another Company Inc', "98 High Valley Road",
        None, None, 'Upbridge', 'XY765ZZ',
        'Bosnia and Herzegovina', '123 456 7890']
    result_length = conn.run('SELECT * FROM dim_counterparty;')
    assert len(result_length) == 4


def test_function_raises_key_error_with_incorrect_data(conn, caplog):
    with pytest.raises(KeyError):
        load_counterparty(
            's3://de-project-test-data/parquet/test-currency.parquet',
            conn
        )
    assert 'load_counterparty was given an incorrect file' in caplog.text


def test_function_raises_index_error_with_incorrect_data(conn, caplog):
    with pytest.raises(IndexError):
        load_counterparty(
            's3://de-project-test-data/parquet/test-sales-order.parquet',
            conn
        )
    assert 'load_counterparty has raised an error' in caplog.text


def test_function_calls_conn_with_correct_SQL_query():
    mock_conn = MagicMock()
    load_counterparty(
        "s3://de-project-test-data/parquet/test-counterparty.parquet",
        mock_conn
        )
    expected_insert_query_list = [
        "INSERT INTO dim_counterparty",
        "counterparty_id",
        "counterparty_legal_name",
        "counterparty_legal_address_line_1",
        "counterparty_legal_address_line_2",
        "counterparty_legal_district",
        "counterparty_legal_city",
        "counterparty_legal_postal_code",
        "counterparty_legal_country",
        "counterparty_legal_phone_number",
        ") VALUES ("
        ]
    assert mock_conn.run.call_count == 8
    assert expected_insert_query_list[0] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[1] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[2] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[3] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[4] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[5] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[6] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[7] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[8] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[9] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[10] in str(mock_conn.run.call_args)


def test_function_handles_DatabaseError(caplog):
    mock_conn = MagicMock()
    mock_conn.run.side_effect = DatabaseError()

    with pytest.raises(DatabaseError):
        load_counterparty(
            's3://de-project-test-data/parquet/test-counterparty.parquet',
            mock_conn)
    assert 'load_counterparty has raised an error' in caplog.text


def test_function_handles_IndexError(caplog):
    mock_conn = MagicMock()
    mock_conn.run.side_effect = IndexError()

    with pytest.raises(IndexError):
        load_counterparty(
            's3://de-project-test-data/parquet/test-counterparty.parquet',
            mock_conn)
    assert 'load_counterparty has raised an error' in caplog.text


def test_function_handles_Exception(caplog):
    mock_conn = MagicMock()
    mock_conn.run.side_effect = Exception()

    with pytest.raises(Exception):
        load_counterparty(
            's3://de-project-test-data/parquet/test-counterparty.parquet',
            mock_conn)
    assert 'load_counterparty has raised an error' in caplog.text
