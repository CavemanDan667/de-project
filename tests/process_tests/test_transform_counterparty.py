from src.process.process_utils.transform_counterparty import (
    transform_counterparty
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


@pytest.fixture
def conn():
    return Connection(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database)


def test_function_returns_a_dataframe(conn):
    result = transform_counterparty(
        'tests/csv_test_files/test-counterparty.csv',
        conn
    )
    assert isinstance(result, pd.core.frame.DataFrame)
