from src.process.process_utils.transform_staff import transform_staff
from pg8000.native import Connection
import pytest
import pandas as pd
from tests.get_credentials import get_credentials
import subprocess
from dotenv import dotenv_values

identity = subprocess.check_output('whoami')

if identity == b'runner\n':
    config = get_credentials('test_totesys_db_creds')
else:
    config = dotenv_values('.env')

dw_user = config["TESTDW_USER"]
dw_password = config["TESTDW_PASSWORD"]
dw_host = config["TESTDW_HOST"]
dw_port = config["TESTDW_PORT"]
dw_database = config["TESTDW_DATABASE"]

mock_dept_data = [
    [1, 'Dept1', 'LocationA'],
    [2, 'Dept2', 'LocationA'],
    [3, 'Dept3', 'LocationB'],
    [4, 'Dept4', 'LocationB']
]


@pytest.fixture
def mock_conn():
    class MockConnection:
        def __init__(self):
            self.name = 'mockconn'

        def run(*args):
            return mock_dept_data
    return MockConnection()


@pytest.fixture
def dw_conn():
    return Connection(
        user=dw_user,
        password=dw_password,
        host=dw_host,
        port=dw_port,
        database=dw_database
    )


def test_function_returns_data_frame(mock_conn, dw_conn):
    result = transform_staff(
        'tests/csv_test_files/test-staff.csv',
        mock_conn, dw_conn
        )
    assert isinstance(result, pd.core.frame.DataFrame)


def test_function_returns_correct_data(mock_conn, dw_conn):
    result = transform_staff(
       'tests/csv_test_files/test-staff.csv',
       mock_conn, dw_conn
    )
    assert result.values.tolist() == [
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
         'namee.surnamee@terrifictotes.com']
    ]


def test_function_correctly_populates_table(mock_conn, dw_conn):
    transform_staff(
       'tests/csv_test_files/test-staff.csv',
       mock_conn, dw_conn
    )
    result = dw_conn.run('SELECT * FROM dim_staff;')
    assert result[0] == [1, 'NameA', 'SurnameA',
                         'Dept2', 'LocationA',
                         'namea.surnamea@terrifictotes.com']
    assert result[1] == [2, 'NameB', 'SurnameB',
                         'Dept1', 'LocationA',
                         'nameb.surnameb@terrifictotes.com']
    assert result[2] == [3, 'NameC', 'SurnameC',
                         'Dept1', 'LocationA',
                         'namec.surnamec@terrifictotes.com']
    assert result[3] == [4, 'NameD', 'SurnameD',
                         'Dept3', 'LocationB',
                         'named.surnamed@terrifictotes.com']
    assert result[4] == [5, 'NameE', 'SurnameE',
                         'Dept4', 'LocationB',
                         'namee.surnamee@terrifictotes.com']


def test_function_does_not_repeat_duplicate_data(mock_conn, dw_conn):
    transform_staff(
       'tests/csv_test_files/test-staff.csv',
       mock_conn, dw_conn
    )
    transform_staff(
       'tests/csv_test_files/test-staff.csv',
       mock_conn, dw_conn
    )
    result = dw_conn.run('SELECT * FROM dim_staff;')
    assert len(result) == 5


def test_function_correctly_updates_data(mock_conn, dw_conn):
    result = transform_staff(
       'tests/csv_test_files/test-staff-update.csv',
       mock_conn, dw_conn
    )
    assert result.values.tolist() == [[
        1,
        'NameA',
        'MarriedSurname',
        'Dept2',
        'LocationA',
        'namea.surnamea@terrifictotes.com'
    ]]
    query = 'SELECT * FROM dim_staff;'
    result = dw_conn.run(query)
    assert result[0] == [2, 'NameB', 'SurnameB', 'Dept1',
                         'LocationA', 'nameb.surnameb@terrifictotes.com']
    assert result[1] == [3, 'NameC', 'SurnameC', 'Dept1',
                         'LocationA', 'namec.surnamec@terrifictotes.com']
    assert result[2] == [4, 'NameD', 'SurnameD', 'Dept3',
                         'LocationB', 'named.surnamed@terrifictotes.com']
    assert result[3] == [5, 'NameE', 'SurnameE', 'Dept4',
                         'LocationB', 'namee.surnamee@terrifictotes.com']
    assert result[4] == [1, 'NameA', 'MarriedSurname', 'Dept2',
                         'LocationA', 'namea.surnamea@terrifictotes.com']


def test_function_correctly_updates_change_of_department(mock_conn, dw_conn):
    result = transform_staff(
       'tests/csv_test_files/test-staff-update-transfer.csv',
       mock_conn, dw_conn
    )
    assert result.values.tolist() == [
        [1,
         'NameA',
         'SurnameA',
         'Dept3',
         'LocationB',
         'namea.surnamea@terrifictotes.com',]]
    query = 'SELECT * FROM dim_staff;'
    result = dw_conn.run(query)
    assert result[0] == [2, 'NameB', 'SurnameB', 'Dept1',
                         'LocationA', 'nameb.surnameb@terrifictotes.com']
    assert result[1] == [3, 'NameC', 'SurnameC', 'Dept1',
                         'LocationA', 'namec.surnamec@terrifictotes.com']
    assert result[2] == [4, 'NameD', 'SurnameD', 'Dept3',
                         'LocationB', 'named.surnamed@terrifictotes.com']
    assert result[3] == [5, 'NameE', 'SurnameE', 'Dept4',
                         'LocationB', 'namee.surnamee@terrifictotes.com']
    assert result[4] == [1, 'NameA', 'SurnameA', 'Dept3',
                         'LocationB', 'namea.surnamea@terrifictotes.com']
