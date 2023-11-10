from src.process.process_utils.transform_staff import transform_staff
from pg8000.native import Connection
import pytest
import pandas as pd
from src.loading.load_utils.get_credentials import get_credentials
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


def test_function_returns_data_frame(mock_conn):
    result = transform_staff(
        'tests/csv_test_files/test-staff.csv',
        mock_conn
        )
    assert isinstance(result, pd.core.frame.DataFrame)


def test_function_returns_correct_data(mock_conn):
    result = transform_staff(
       'tests/csv_test_files/test-staff.csv',
       mock_conn
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


def test_function_only_returns_new_data(mock_conn):
    result = transform_staff(
        'tests/csv_test_files/test-staff-2.csv',
        mock_conn
    )
    assert result.values.tolist() == [[6, 'NameF', 'SurnameF',
                                       'Dept2', 'LocationA',
                                       'namef.surnamef@terrifictotes.com']]
