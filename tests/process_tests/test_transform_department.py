from src.process.process_utils.transform_department import transform_department
from pg8000.native import Connection
import pytest
import pandas as pd
from tests.get_credentials import get_credentials

config = get_credentials('test_dw_creds')

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


def test_function_returns_data_frame(conn):
    result = transform_department(
        'tests/csv_test_files/test-department.csv',
        conn
        )
    assert isinstance(result, pd.core.frame.DataFrame)


def test_function_returns_correct_data(conn):
    result = transform_department(
       'tests/csv_test_files/test-department.csv',
       conn
    )
    assert result.values.tolist() == [
        [1, 'Dept1', 'LocationA'],
        [2, 'Dept2', 'LocationA'],
        [3, 'Dept3', 'LocationB'],
        [4, 'Dept4', 'LocationB']
    ]


def test_function_correctly_populates_table(conn):
    transform_department(
       'tests/csv_test_files/test-department.csv',
       conn
    )
    result = conn.run('SELECT * FROM ref_department;')
    assert result[0] == [1, 'Dept1', 'LocationA']
    assert result[1] == [2, 'Dept2', 'LocationA']
    assert result[2] == [3, 'Dept3', 'LocationB']
    assert result[3] == [4, 'Dept4', 'LocationB']


def test_function_does_not_repeat_duplicate_data(conn):
    transform_department(
       'tests/csv_test_files/test-department.csv',
       conn
    )
    transform_department(
       'tests/csv_test_files/test-department.csv',
       conn
    )
    result = conn.run('SELECT * FROM ref_department;')
    assert len(result) == 4


def test_function_correctly_updates_data(conn):
    result = transform_department(
       'tests/csv_test_files/test-department-update.csv',
       conn
    )
    assert result.values.tolist() == [[1, 'Dept1', 'LocationF']]
    query = 'SELECT * FROM ref_department;'
    result_data = conn.run(query)
    assert result_data == [
        [2, 'Dept2', 'LocationA'],
        [3, 'Dept3', 'LocationB'],
        [4, 'Dept4', 'LocationB'],
        [1, 'Dept1', 'LocationF']]
