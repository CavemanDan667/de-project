from src.process.transform_staff import transform_staff
from pg8000.native import Connection, DatabaseError
from dotenv import dotenv_values
import pytest

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
    result = transform_staff(
        'tests/csv_test_files/test-staff.csv',
        'tests/csv_test_files/test-department.csv',
        conn
    )
    assert result.values.tolist()[0:3] == [
        ['NameA',
         'SurnameA',
         'namea.surnamea@terrifictotes.com',
         'Dept2', 'LocationA'],
        ['NameB',
         'SurnameB',
         'nameb.surnameb@terrifictotes.com',
         'Dept1',
         'LocationA'],
        ['NameC',
         'SurnameC',
         'namec.surnamec@terrifictotes.com',
         'Dept1',
         'LocationA']
    ]


def test_function_raises_database_error_if_query_fails():
    conn = Connection(
        user=config["TEST_USER"],
        password=config["TEST_PASSWORD"],
        host=config["TEST_HOST"],
        port=config["TEST_PORT"],
        database=config["TEST_DATABASE"]
    )
    with pytest.raises(DatabaseError):
        transform_staff(
            'tests/csv_test_files/test-staff.csv',
            'tests/csv_test_files/test-department.csv',
            conn
        )
