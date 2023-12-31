from src.loading.load_utils.load_staff import load_staff
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
    result = load_staff(
       's3://de-project-test-data/parquet/staff-test.parquet',
       conn
    )
    assert result == 'Data loaded successfully - dim_staff'


def test_function_correctly_populates_table(conn):
    load_staff(
        's3://de-project-test-data/parquet/staff-test.parquet',
        conn
    )
    load_staff(
        's3://de-project-test-data/parquet/staff-test2.parquet',
        conn
    )

    result = conn.run('SELECT * FROM dim_staff ORDER BY staff_id;')

    assert result == [
        [1, 'NameA', 'SurnameA',
         'Dept2', 'LocationA',
         'namea.surnamea@terrifictotes.com'],
        [2, 'NameB', 'SurnameB',
         'Dept1', 'LocationA',
         'nameb.surnameb@terrifictotes.com'],
        [3, 'NameC', 'SurnameC',
         'Dept1', 'LocationA',
         'namec.surnamec@terrifictotes.com'],
        [4, 'NameD', 'SurnameD',
         'Dept3', 'LocationB',
         'named.surnamed@terrifictotes.com',],
        [5, 'NameE', 'SurnameE',
         'Dept4', 'LocationB',
         'namee.surnamee@terrifictotes.com'],
        [6, 'NameF', 'SurnameF',
         'Dept2', 'LocationA',
         'namef.surnamef@terrifictotes.com'],
    ]


def test_function_does_not_duplicate_data(conn):

    load_staff(
        's3://de-project-test-data/parquet/staff-test.parquet',
        conn
    )
    load_staff(
        's3://de-project-test-data/parquet/staff-test.parquet',
        conn
    )
    load_staff(
        's3://de-project-test-data/parquet/staff-test2.parquet',
        conn
    )
    load_staff(
        's3://de-project-test-data/parquet/staff-test2.parquet',
        conn
        )
    load_staff(
        's3://de-project-test-data/parquet/staff-test2.parquet',
        conn
        )
    result = conn.run('SELECT * FROM dim_staff;')
    assert len(result) == 6


def test_function_correctly_updates_data(conn):

    load_staff(
        's3://de-project-test-data/parquet/staff-update.parquet',
        conn
    )
    result = conn.run('SELECT * FROM dim_staff ORDER BY staff_id;')
    assert result == [
        [1, 'NameA', 'MarriedSurname',
         'Dept2', 'LocationA',
         'namea.surnamea@terrifictotes.com'],
        [2, 'NameB', 'SurnameB',
         'Dept1', 'LocationA',
         'nameb.surnameb@terrifictotes.com'],
        [3, 'NameC', 'SurnameC',
         'Dept1', 'LocationA',
         'namec.surnamec@terrifictotes.com'],
        [4, 'NameD', 'SurnameD',
         'Dept3', 'LocationB',
         'named.surnamed@terrifictotes.com',],
        [5, 'NameE', 'SurnameE',
         'Dept4', 'LocationB',
         'namee.surnamee@terrifictotes.com'],
        [6, 'NameF', 'SurnameF',
         'Dept2', 'LocationA',
         'namef.surnamef@terrifictotes.com'],
    ]


def test_function_correctly_updates_department(conn):

    load_staff(
        's3://de-project-test-data/parquet/staff-update-transfer.parquet',
        conn
    )
    result = conn.run(
        'SELECT * FROM dim_staff ORDER BY staff_id;')
    assert result == [
        [1, 'NameA', 'SurnameA',
         'Dept3', 'LocationB',
         'namea.surnamea@terrifictotes.com'],
        [2, 'NameB', 'SurnameB',
         'Dept1', 'LocationA',
         'nameb.surnameb@terrifictotes.com'],
        [3, 'NameC', 'SurnameC',
         'Dept1', 'LocationA',
         'namec.surnamec@terrifictotes.com'],
        [4, 'NameD', 'SurnameD',
         'Dept3', 'LocationB',
         'named.surnamed@terrifictotes.com',],
        [5, 'NameE', 'SurnameE',
         'Dept4', 'LocationB',
         'namee.surnamee@terrifictotes.com'],
        [6, 'NameF', 'SurnameF',
         'Dept2', 'LocationA',
         'namef.surnamef@terrifictotes.com']
        ]


def test_function_returns_key_error_with_incorrect_data(conn):
    with pytest.raises(KeyError):
        load_staff(
            "s3://de-project-test-data/parquet/test-currency.parquet",
            conn
        )


def test_function_calls_conn_with_correct_SQL_query():
    mock_conn = MagicMock()
    load_staff(
        "s3://de-project-test-data/parquet/staff-test.parquet",
        mock_conn
        )
    expected_insert_query_list = [
        "staff_id,",
        "first_name,",
        "last_name,",
        "department_name,",
        "location,",
        "email_address",
        ") VALUES ("
        ]
    assert mock_conn.run.call_count == 10
    assert expected_insert_query_list[0] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[1] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[2] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[3] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[4] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[5] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[6] in str(mock_conn.run.call_args)


def test_function_handles_DatabaseError(caplog):
    mock_conn = MagicMock()
    mock_conn.run.side_effect = DatabaseError()

    with pytest.raises(DatabaseError):
        load_staff(
            's3://de-project-test-data/parquet/staff-update.parquet',
            mock_conn)
    assert 'load_staff has raised an error' in caplog.text


def test_function_handles_ValueError(caplog):
    mock_conn = MagicMock()
    mock_conn.run.side_effect = ValueError()

    with pytest.raises(ValueError):
        load_staff(
            's3://de-project-test-data/parquet/staff-update.parquet',
            mock_conn)
    assert 'load_staff has raised an error' in caplog.text


def test_function_handles_IndexError(caplog):
    mock_conn = MagicMock()
    mock_conn.run.side_effect = IndexError()

    with pytest.raises(IndexError):
        load_staff(
            's3://de-project-test-data/parquet/staff-update.parquet',
            mock_conn)
    assert 'load_staff has raised an error' in caplog.text
