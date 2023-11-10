from src.loading.load_utils.load_counterparty import load_counterparty
from src.loading.load_utils.get_credentials import get_credentials
from src.loading.load_utils.load_address import (
    load_address
)
from pg8000.native import Connection
from dotenv import dotenv_values
import pytest
import subprocess


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


@pytest.fixture()
def conn():
    return Connection(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database)


def test_function_returns_success_message(conn):
    load_address(
        "s3://de-project-test-data/parquet/test-address.parquet",
        conn
    )
    result = load_counterparty(
       's3://de-project-test-data/parquet/counterparty.parquet',
       conn
    )
    assert result == 'Data loaded successfully - dim_counterparty'


def test_function_inserts_data_into_table(conn):
    load_address(
        "s3://de-project-test-data/parquet/test-address.parquet",
        conn
    )
    result = load_counterparty(
       's3://de-project-test-data/parquet/counterparty2.parquet',
       conn
    )
    result = conn.run('SELECT * FROM dim_counterparty;')
    assert result[0] == [
        1, 'Another Company Inc', '5 Far Lane',
        'Parkway', 'North Shore', 'Castletown',
        'AB2 3CD', 'Wales', '1234 800900']
    assert result[1] == [
        2, 'Clarke, Hunter and Lorimer',
        "234 St. Steven's Road", 'None', 'None', 'Old Town',
        '22222-3333', 'Northern Ireland', '07700 100200']
    assert len(result) == 2


def test_function_does_not_duplicate_data(conn):
    load_address(
        "s3://de-project-test-data/parquet/test-address.parquet",
        conn
    )
    result = load_counterparty(
       's3://de-project-test-data/parquet/counterparty2.parquet',
       conn
    )
    result = load_counterparty(
       's3://de-project-test-data/parquet/counterparty2.parquet',
       conn
    )
    result = conn.run('SELECT * FROM dim_counterparty;')
    assert len(result) == 2


def test_function_can_update_data(conn):
    load_address(
        "s3://de-project-test-data/parquet/test-address.parquet",
        conn
    )
    result = load_counterparty(
       's3://de-project-test-data/parquet/counterparty3.parquet',
       conn
    )
    result = conn.run('SELECT * FROM dim_counterparty;')
    assert result[1] == [
        2, 'Clarke, Hunter and Lorimer', "234 St. Steven's Road",
        'None', 'None', 'New Town', '22222-3333',
        'Northern Ireland', '07700 100200']
