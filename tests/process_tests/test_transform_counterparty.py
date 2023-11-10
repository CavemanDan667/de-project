from src.process.process_utils.transform_counterparty import (
    transform_counterparty
)
from src.loading.load_utils.load_address import (
    load_address
)
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
    load_address(
        "tests/parquet_test_files/test-address.parquet", conn)
    result = transform_counterparty(
        'tests/csv_test_files/test-counterparty.csv',
        conn
    )
    assert isinstance(result, pd.core.frame.DataFrame)


def test_function_finds_correct_address_data(conn):
    load_address(
        "tests/parquet_test_files/test-address.parquet",
        conn
    )
    result = transform_counterparty(
        'tests/csv_test_files/test-counterparty.csv',
        conn
    )
    assert result.values.tolist()[3] == [
        4,
        'Another Company Inc',
        '5 Far Lane',
        'Parkway',
        'North Shore',
        'Castletown',
        'AB2 3CD',
        'Wales',
        '1234 800900'
    ]


def test_function_deals_with_empty_address_values(conn):
    load_address(
        "tests/parquet_test_files/test-address.parquet",
        conn
    )
    result = transform_counterparty(
        'tests/csv_test_files/test-counterparty.csv',
        conn
    )
    assert result.values.tolist()[0] == [
        1,
        'Company and Sons',
        "234 St. Steven's Road",
        None,
        None,
        'Old Town',
        '22222-3333',
        'Northern Ireland',
        '07700 100200'
    ]
