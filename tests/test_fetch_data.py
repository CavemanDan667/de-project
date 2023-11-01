from src.fetch_data import fetch_data
from src.connection import get_connection
from dotenv import dotenv_values
import pytest

config = dotenv_values(".env.test")

user = config["USER"]
password = config["PASSWORD"]
host = config["HOST"]
port = config["PORT"]
database = config["DATABASE"]


@pytest.fixture
def conn():
    return get_connection(user, password, host, port, database)


def test_function_returns_data_in_a_dictionary(conn):
    result = fetch_data(conn, 'table_1')
    assert result == {
        'Headers': ['column_1', 'column_2', 'column_3'],
        'Rows': [
            [1, 'one', 10],
            [2, 'two', 20],
            [3, 'three', 30]
        ]
    }


def test_function_returns_just_headers_from_unpopulated_table(conn):
    result = fetch_data(conn, 'table_2')
    assert result == {
        'Headers': ['column_a', 'column_b', 'column_c'],
        'Rows': []
    }


def test_function_returns_error_message_on_nonexistent_table(conn):
    result = fetch_data(conn, 'table_x')
    assert result == 'Table not found'


def test_function_returns_two_empty_lists_for_empty_table(conn):
    result = fetch_data(conn, '_table_3')
    assert result == {
        'Headers': [],
        'Rows': []
    }


def test_raises_error_if_parameters_incorrect_or_missing(conn):
    with pytest.raises(TypeError):
        fetch_data(conn)
