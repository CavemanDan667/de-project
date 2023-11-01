from src.ingestion.ingestion_utils.get_tables import get_table_names
import pytest
from pg8000.native import Connection


def test_function_raises_error_if_no_connection_passed():
    with pytest.raises(TypeError):
        get_table_names()


def test_function_returns_table_names_in_a_list():
    mock_conn = Connection(
        user='helen',
        host='localhost',
        port=5432,
        database='mock_tote_db'
    )
    result = get_table_names(mock_conn)
    assert result == ['table_1', 'table_2']


def test_function_ignores_private_table_names():
    mock_conn = Connection(
        user='helen',
        host='localhost',
        port=5432,
        database='mock_tote_db'
    )
    result = get_table_names(mock_conn)
    assert result == ['table_1', 'table_2']


def test_returns_empty_list_if_database_is_empty():
    empty_conn = Connection(
        user='helen',
        host='localhost',
        port=5432,
        database='mock_empty_db'
    )
    result = get_table_names(empty_conn)
    assert result == []
