from src.loading.load_utils.load_address import load_address
from src.loading.load_utils.get_credentials import get_credentials
from pg8000.native import Connection
from dotenv import dotenv_values
import pytest
import subprocess

identity = subprocess.check_output("whoami")

if identity == b"runner\n":
    config = get_credentials("test_totesys_db_creds")
else:
    config = dotenv_values(".env")

user = config["TESTDW_USER"]
password = config["TESTDW_PASSWORD"]
host = config["TESTDW_HOST"]
port = config["TESTDW_PORT"]
database = config["TESTDW_DATABASE"]


@pytest.fixture()
def conn():
    return Connection(
        user=user, password=password, host=host, port=port, database=database
    )


def test_function_returns_success_message(conn):
    result = load_address(
        "s3://de-project-test-data/parquet/test-address.parquet", conn)
    assert result == "Data loaded successfully - dim_location"


def test_function_correctly_populates_table_replacing_blanks_with_none(conn):
    load_address(
        "s3://de-project-test-data/parquet/test-address.parquet",
        conn
    )
    result = conn.run("SELECT * FROM dim_location;")
    assert result[0] == [
        1,
        "123 Main Street",
        None,
        "Central",
        "New Town",
        "12345",
        "England",
        "1234 567890",
    ]
    assert result[1] == [
        2,
        "234 St. Steven's Road",
        None,
        None,
        "Old Town",
        "22222-3333",
        "Northern Ireland",
        "07700 100200",
    ]
    assert result[2] == [
        3,
        "Flat 12",
        "Block A",
        None,
        "New South Bridge",
        "67890",
        "Scotland",
        "0044 123456",
    ]
    assert result[3] == [
        4,
        "5 Far Lane",
        "Parkway",
        "North Shore",
        "Castletown",
        "AB2 3CD",
        "Wales",
        "1234 800900",
    ]
    assert result[4] == [
        5,
        "98 High Valley Road",
        None,
        None,
        "Upbridge",
        "XY765ZZ",
        "Bosnia and Herzegovina",
        "123 456 7890",
    ]


def test_function_does_not_duplicate_data(conn):
    load_address(
        "s3://de-project-test-data/parquet/test-address.parquet",
        conn
    )
    load_address(
        "s3://de-project-test-data/parquet/test-address.parquet",
        conn
    )
    result = conn.run("SELECT * FROM dim_location;")
    assert len(result) == 5
