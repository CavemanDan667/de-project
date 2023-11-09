from src.process.process_utils.transform_payment_type import (
    transform_payment_type
)
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


@pytest.fixture()
def conn():
    return Connection(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database)


def test_function_returns_data_frame(conn):
    result = transform_payment_type(
        'tests/csv_test_files/test-payment-type.csv',
        conn
        )
    assert isinstance(result, pd.core.frame.DataFrame)


def test_function_returns_correct_data(conn):
    result = transform_payment_type(
       'tests/csv_test_files/test-payment-type.csv',
       conn
    )
    assert result.values.tolist() == [
        [1, 'TYPE_ONE'],
        [2, 'TYPE_TWO'],
        [3, 'TYPE_THREE'],
        [4, 'TYPE_FOUR']
    ]


def test_function_only_returns_new_data(conn):
    result = transform_payment_type(
       'tests/csv_test_files/test-payment-type-plus.csv',
       conn
    )
    assert result.values.tolist() == [
        [5, 'TYPE_FIVE']
    ]


def test_function_correctly_populates_table(conn):
    transform_payment_type(
       'tests/csv_test_files/test-payment-type.csv',
       conn
    )
    transform_payment_type(
       'tests/csv_test_files/test-payment-type-plus.csv',
       conn
    )
    result = conn.run('SELECT * FROM dim_payment_type;')
    assert result[0] == [1, 'TYPE_ONE']
    assert result[1] == [2, 'TYPE_TWO']
    assert result[2] == [3, 'TYPE_THREE']
    assert result[3] == [4, 'TYPE_FOUR']
    assert result[4] == [5, 'TYPE_FIVE']


def test_function_does_not_repeat_duplicate_data(conn):
    transform_payment_type(
       'tests/csv_test_files/test-payment-type.csv',
       conn
    )
    transform_payment_type(
       'tests/csv_test_files/test-payment-type.csv',
       conn
    )
    transform_payment_type(
       'tests/csv_test_files/test-payment-type-plus.csv',
       conn
    )
    transform_payment_type(
       'tests/csv_test_files/test-payment-type-plus.csv',
       conn
    )
    transform_payment_type(
       'tests/csv_test_files/test-payment-type-plus.csv',
       conn
    )
    result = conn.run('SELECT * FROM dim_payment_type;')
    assert len(result) == 5
