from src.ingestion.ingestion_utils.fetch_data import fetch_data
from pg8000.native import Connection, DatabaseError
import pytest
from datetime import datetime as dt
from src.ingestion.ingestion_utils.get_credentials import get_credentials
import subprocess
from dotenv import dotenv_values

identity = subprocess.check_output('whoami')

if identity == b'runner\n':
    config = get_credentials('test_totesys_db_creds')
else:
    config = dotenv_values('.env')

user = config["TEST_USER"]
password = config["TEST_PASSWORD"]
host = config["TEST_HOST"]
port = config["TEST_PORT"]
database = config["TEST_DATABASE"]


@pytest.fixture
def conn():
    return Connection(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database)


def test_function_returns_all_data_if_newest_time_0(conn):
    result = fetch_data(
        conn,
        'table_1',
        '1970-01-01 00:00:00.000',
        '2023-11-10 00:00:00.000'
    )
    assert result == {
        'Headers': ['column_1', 'column_2', 'column_3', 'last_updated'],
        'Rows': [
            [1, 'one', 10, dt(2020, 1, 1, 0, 0)],
            [2, 'two', 20, dt(2020, 1, 1, 0, 0)],
            [3, 'three', 30, dt(2023, 11, 1, 0, 0)]
        ]
    }


def test_function_returns_just_new_data_if_given_newest_time(conn):
    result = fetch_data(
        conn,
        'table_1',
        '2022-01-01 00:00:00',
        '2023-11-10 00:00:00'
    )
    assert result == {
        'Headers': ['column_1', 'column_2', 'column_3', 'last_updated'],
        'Rows': [
            [3, 'three', 30, dt(2023, 11, 1, 0, 0)]
        ]
    }


def test_function_returns_just_headers_from_unpopulated_table(conn):
    result = fetch_data(
        conn,
        'table_2',
        '1970-01-01 00:00:00.000',
        '2023-11-10 00:00:00'
    )
    assert result == {
        'Headers': ['column_a', 'column_b', 'column_c', 'last_updated'],
        'Rows': []
    }


def test_function_returns_error_message_on_nonexistent_table(conn, caplog):
    with pytest.raises(DatabaseError):
        fetch_data(
            conn,
            'table_x',
            '1970-01-01 00:00:00.000',
            '2023-11-10 00:00:00'
        )
    assert 'There was a database error' in caplog.text


def test_function_returns_error_message_for_empty_table(conn, caplog):
    with pytest.raises(DatabaseError):
        fetch_data(
            conn,
            '_table_3',
            '1970-01-01 00:00:00.000',
            '2023-11-10 00:00:00'
        )
    assert 'There was a database error' in caplog.text


def test_raises_error_if_parameters_incorrect_or_missing(conn):
    with pytest.raises(TypeError):
        fetch_data(conn)
