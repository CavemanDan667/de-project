from src.loading.load_utils.load_address import load_address
from src.loading.load_utils.get_credentials import get_credentials
from pg8000.native import Connection, DatabaseError
from dotenv import dotenv_values
import pytest
import subprocess
from unittest.mock import MagicMock

identity = subprocess.check_output("whoami")

if identity == b"runner\n":
    config = get_credentials("test_dw_creds")
else:
    config = dotenv_values(".env")

user = config["TESTDW_USER"]
password = config["TESTDW_PASSWORD"]
host = config["TESTDW_HOST"]
port = config["TESTDW_PORT"]
database = config["TESTDW_DATABASE"]


@pytest.fixture()
def conn():
    return Connection(
        user=user, password=password, host=host, port=port, database=database
    )


def test_function_returns_success_message(conn):
    result = load_address(
        "s3://de-project-test-data/parquet/test-address.parquet", conn)
    assert result == "Data loaded successfully - dim_location"


def test_function_correctly_populates_table_replacing_blanks_with_none(conn):
    load_address(
        "s3://de-project-test-data/parquet/test-address.parquet",
        conn
    )
    result = conn.run("SELECT * FROM dim_location;")
    assert result[0] == [
        1,
        "123 Main Street",
        None,
        "Central",
        "New Town",
        "12345",
        "England",
        "1234 567890",
    ]
    assert result[1] == [
        2,
        "234 St. Steven's Road",
        None,
        None,
        "Old Town",
        "22222-3333",
        "Northern Ireland",
        "07700 100200",
    ]
    assert result[2] == [
        3,
        "Flat 12",
        "Block A",
        None,
        "New South Bridge",
        "67890",
        "Scotland",
        "0044 123456",
    ]
    assert result[3] == [
        4,
        "5 Far Lane",
        "Parkway",
        "North Shore",
        "Castletown",
        "AB2 3CD",
        "Wales",
        "1234 800900",
    ]
    assert result[4] == [
        5,
        "98 High Valley Road",
        None,
        None,
        "Upbridge",
        "XY765ZZ",
        "Bosnia and Herzegovina",
        "123 456 7890",
    ]


def test_function_does_not_duplicate_data(conn):
    load_address(
        "s3://de-project-test-data/parquet/test-address.parquet",
        conn
    )
    load_address(
        "s3://de-project-test-data/parquet/test-address.parquet",
        conn
    )
    result = conn.run("SELECT * FROM dim_location;")
    assert len(result) == 5


def test_function_returns_key_error_with_incorrect_data(conn, caplog):
    with pytest.raises(KeyError):
        load_address(
            "s3://de-project-test-data/parquet/test-currency.parquet",
            conn
        )
    assert 'load_address was given an incorrect file' in caplog.text


def test_function_calls_conn_with_correct_SQL_query():
    mock_conn = MagicMock()
    load_address(
        "s3://de-project-test-data/parquet/test-address.parquet",
        mock_conn
        )
    expected_insert_query_list = [
        "INSERT INTO dim_location (",
        "location_id, address_line_1,",
        "address_line_2, district,",
        "city, postal_code,",
        "country, phone",
        ") VALUES (",
        ]
    assert mock_conn.run.call_count == 10
    assert expected_insert_query_list[0] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[1] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[2] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[3] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[4] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[5] in str(mock_conn.run.call_args)


def test_function_raises_DatabaseError(caplog):
    mock_conn = MagicMock()
    mock_conn.run.side_effect = DatabaseError()
    with pytest.raises(DatabaseError):
        load_address(
            's3://de-project-test-data/parquet/test-address.parquet',
            mock_conn
            )
    assert 'load_address has raised a database error' in caplog.text


def test_function_raises_IndexError(caplog):
    mock_conn = MagicMock()
    mock_conn.run.side_effect = IndexError()
    with pytest.raises(IndexError):
        load_address(
            's3://de-project-test-data/parquet/test-address.parquet',
            mock_conn
            )
    assert 'load_address has raised an error' in caplog.text
