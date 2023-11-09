from src.ingestion.ingestion_utils.get_tables import get_table_names
import pytest
from pg8000.native import Connection
from tests.get_credentials import get_credentials
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


def test_function_raises_error_if_no_connection_passed():
    with pytest.raises(TypeError):
        get_table_names()


def test_function_returns_table_names_in_a_list():
    mock_conn = Connection(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database
    )
    result = get_table_names(mock_conn)
    assert result == ['table_1', 'table_2']


def test_function_ignores_private_table_names():
    mock_conn = Connection(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database
    )
    result = get_table_names(mock_conn)
    assert result == ['table_1', 'table_2']


def test_returns_empty_list_if_database_is_empty():
    empty_conn = Connection(
        user=user,
        password=password,
        host=host,
        port=port,
        database='mock_empty_db'
    )
    result = get_table_names(empty_conn)
    assert result == []
