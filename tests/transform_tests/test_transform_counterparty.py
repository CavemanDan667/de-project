from src.transform.transform_utils.transform_counterparty import (
    transform_counterparty
)
from src.loading.load_utils.load_address import (
    load_address
)
from pg8000.native import Connection
import pandas as pd
import pytest
from src.transform.transform_utils.get_credentials import get_credentials
import subprocess
from dotenv import dotenv_values

identity = subprocess.check_output('whoami')

if identity == b'runner\n':
    config = get_credentials('test_dw_creds')
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
        database=database
    )


def test_function_returns_a_dataframe(conn):
    load_address(
        "s3://de-project-test-data/parquet/test-address.parquet", conn)
    result = transform_counterparty(
        's3://de-project-test-data/csv/test-counterparty.csv',
        conn
    )
    assert isinstance(result, pd.core.frame.DataFrame)


def test_function_finds_correct_address_data(conn):
    load_address(
        "s3://de-project-test-data/parquet/test-address.parquet",
        conn
    )
    result = transform_counterparty(
        's3://de-project-test-data/csv/test-counterparty.csv',
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
        "s3://de-project-test-data/parquet/test-address.parquet",
        conn
    )
    result = transform_counterparty(
        's3://de-project-test-data/csv/test-counterparty.csv',
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


def test_function_raises_key_error_with_non_matching_address_id(conn):
    with pytest.raises(KeyError):
        transform_counterparty(
            "s3://de-project-test-data/csv/test-rogue-counterparty.csv",
            conn
        )


def test_function_raises_value_error_with_incorrect_file(conn):
    with pytest.raises(ValueError):
        transform_counterparty(
            "s3://de-project-test-data/csv/test-currency.csv",
            conn
        )
