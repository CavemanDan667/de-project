from src.process.process_utils.transform_staff import transform_staff
from src.process.process_utils.transform_department import transform_department
from pg8000.native import Connection
from dotenv import dotenv_values
import pytest
import pandas as pd

config = dotenv_values(".env")

user = config["TESTDW_USER"]
password = config["TESTDW_PASSWORD"]
host = config["TESTDW_HOST"]
port = config["TESTDW_PORT"]
database = config["TESTDW_DATABASE"]


@pytest.fixture
def conn():
    return Connection(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database
    )


def test_function_returns_data_frame(conn):
    transform_department(
       'tests/csv_test_files/test-department-update.csv',
       conn
    )
    result = transform_staff(
        'tests/csv_test_files/test-staff.csv',
        conn
        )
    assert isinstance(result, pd.core.frame.DataFrame)


def test_function_returns_correct_data(conn):
    transform_department(
       'tests/csv_test_files/test-department-update.csv',
       conn
    )
    result = transform_staff(
       'tests/csv_test_files/test-staff.csv',
       conn
    )
    assert result.values.tolist() == [
        [1, 'NameA', 'SurnameA',
         'namea.surnamea@terrifictotes.com', 'Dept2', 'LocationA'],
        [2, 'NameB', 'SurnameB',
         'nameb.surnameb@terrifictotes.com', 'Dept1', 'LocationF'],
        [3, 'NameC', 'SurnameC',
         'namec.surnamec@terrifictotes.com', 'Dept1', 'LocationF'],
        [4, 'NameD', 'SurnameD',
         'named.surnamed@terrifictotes.com', 'Dept3', 'LocationB'],
        [5, 'NameE', 'SurnameE',
         'namee.surnamee@terrifictotes.com', 'Dept4', 'LocationB']
    ]


def test_function_correctly_populates_table(conn):
    transform_department(
       'tests/csv_test_files/test-department-update.csv',
       conn
    )
    transform_staff(
       'tests/csv_test_files/test-staff.csv',
       conn
    )
    result = conn.run('SELECT * FROM dim_staff;')
    assert result[0] == [1, 'NameA', 'SurnameA',
                         'Dept2', 'LocationA',
                         'namea.surnamea@terrifictotes.com']
    assert result[1] == [2, 'NameB', 'SurnameB',
                         'Dept1', 'LocationF',
                         'nameb.surnameb@terrifictotes.com']
    assert result[2] == [3, 'NameC', 'SurnameC',
                         'Dept1', 'LocationF',
                         'namec.surnamec@terrifictotes.com']
    assert result[3] == [4, 'NameD', 'SurnameD',
                         'Dept3', 'LocationB',
                         'named.surnamed@terrifictotes.com']
    assert result[4] == [5, 'NameE', 'SurnameE',
                         'Dept4', 'LocationB',
                         'namee.surnamee@terrifictotes.com']


def test_function_does_not_repeat_duplicate_data(conn):
    transform_department(
       'tests/csv_test_files/test-department-update.csv',
       conn
    )
    transform_staff(
       'tests/csv_test_files/test-staff.csv',
       conn
    )
    transform_staff(
       'tests/csv_test_files/test-staff.csv',
       conn
    )
    result = conn.run('SELECT * FROM dim_staff;')
    assert len(result) == 5


def test_function_correctly_updates_data(conn):
    transform_department(
       'tests/csv_test_files/test-department-update.csv',
       conn
    )
    result = transform_staff(
       'tests/csv_test_files/test-staff-update.csv',
       conn
    )
    assert result.values.tolist() == [
        [1,
         'NameA',
         'MarriedSurname',
         'namea.surnamea@terrifictotes.com',
         'Dept2',
         'LocationA']]
    query = 'SELECT * FROM dim_staff;'
    result = conn.run(query)
    assert result[0] == [2, 'NameB', 'SurnameB', 'Dept1',
                         'LocationF', 'nameb.surnameb@terrifictotes.com']
    assert result[1] == [3, 'NameC', 'SurnameC', 'Dept1',
                         'LocationF', 'namec.surnamec@terrifictotes.com']
    assert result[2] == [4, 'NameD', 'SurnameD', 'Dept3',
                         'LocationB', 'named.surnamed@terrifictotes.com']
    assert result[3] == [5, 'NameE', 'SurnameE', 'Dept4',
                         'LocationB', 'namee.surnamee@terrifictotes.com']
    assert result[4] == [1, 'NameA', 'MarriedSurname', 'Dept2',
                         'LocationA', 'namea.surnamea@terrifictotes.com']


def test_function_correctly_updates_change_of_department(conn):
    transform_department(
       'tests/csv_test_files/test-department-update.csv',
       conn
    )
    result = transform_staff(
       'tests/csv_test_files/test-staff-update-transfer.csv',
       conn
    )
    assert result.values.tolist() == [
        [1,
         'NameA',
         'SurnameA',
         'namea.surnamea@terrifictotes.com',
         'Dept3',
         'LocationB']]
    query = 'SELECT * FROM dim_staff;'
    result = conn.run(query)
    assert result[0] == [2, 'NameB', 'SurnameB', 'Dept1',
                         'LocationF', 'nameb.surnameb@terrifictotes.com']
    assert result[1] == [3, 'NameC', 'SurnameC', 'Dept1',
                         'LocationF', 'namec.surnamec@terrifictotes.com']
    assert result[2] == [4, 'NameD', 'SurnameD', 'Dept3',
                         'LocationB', 'named.surnamed@terrifictotes.com']
    assert result[3] == [5, 'NameE', 'SurnameE', 'Dept4',
                         'LocationB', 'namee.surnamee@terrifictotes.com']
    assert result[4] == [1, 'NameA', 'SurnameA', 'Dept3',
                         'LocationB', 'namea.surnamea@terrifictotes.com']
