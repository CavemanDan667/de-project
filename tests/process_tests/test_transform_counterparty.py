from src.process.process_utils.transform_counterparty import (
    transform_counterparty
)
from src.process.process_utils.transform_address import (
    transform_address
)
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


def test_function_finds_correct_address_data(conn):
    transform_address(
        'tests/csv_test_files/test-address.csv',
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
    transform_address(
        'tests/csv_test_files/test-address.csv',
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


def test_function_inserts_data_into_table(conn):
    transform_address(
        'tests/csv_test_files/test-address.csv',
        conn
    )
    transform_counterparty(
        'tests/csv_test_files/test-counterparty.csv',
        conn
    )
    result = conn.run('SELECT * FROM dim_counterparty;')
    assert result[0] == [
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
    assert result[1] == [
        2,
        'Clarke, Hunter and Lorimer',
        '123 Main Street',
        None,
        'Central',
        'New Town',
        '12345',
        'England',
        '1234 567890'
    ]
    assert len(result) == 4


def test_function_does_not_duplicate_data(conn):
    transform_address(
        'tests/csv_test_files/test-address.csv',
        conn
    )
    transform_counterparty(
        'tests/csv_test_files/test-counterparty.csv',
        conn
    )
    transform_counterparty(
        'tests/csv_test_files/test-counterparty.csv',
        conn
    )
    result = conn.run('SELECT * FROM dim_counterparty;')
    assert len(result) == 4


def test_function_can_update_data(conn):
    transform_address(
        'tests/csv_test_files/test-address.csv',
        conn
    )
    transform_counterparty(
        'tests/csv_test_files/test-counterparty-update.csv',
        conn
    )
    result = conn.run('SELECT * FROM dim_counterparty;')
    assert result[3] == [
        4,
        'Another Company Inc',
        '98 High Valley Road',
        None,
        None,
        'Upbridge',
        'XY765ZZ',
        'Bosnia and Herzegovina',
        '123 456 7890'
    ]
