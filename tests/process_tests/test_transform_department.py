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
