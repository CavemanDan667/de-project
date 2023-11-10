from src.loading.load_utils.load_design import load_design
from tests.get_credentials import get_credentials
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
    result = load_design("tests/parquet_test_files/test-design.parquet", conn)
    assert result == "Data loaded successfully - dim_design"


def test_function_correctly_populates_table(conn):
    load_design("tests/parquet_test_files/test-design.parquet", conn)
    result = conn.run("SELECT * FROM dim_design;")
    assert result[0] == [18, "Name1", "/usr", "name1-20000101-abcd.json"]
    assert result[1] == [29, "Name2", "/private", "name2-20000101-4eff.json"]
    assert result[2] == [345, "Name3",
                         "/private/var", "name3-20000101-3ghj.json"]
    assert result[3] == [4, "Name3",
                         "/private/var", "name3-20000101-klmn.json"]
    assert result[4] == [52, "Name2",
                         "/lost+found", "name2-20000101-p123.json"]


def test_function_correctly_updates_data(conn):
    load_design("tests/parquet_test_files/test-design-update.parquet", conn)
    query = "SELECT * FROM dim_design;"
    result_data = conn.run(query)
    assert result_data == [
        [18, "Name1", "/usr", "name1-20000101-abcd.json"],
        [29, "Name2", "/private", "name2-20000101-4eff.json"],
        [345, "Name3", "/private/var", "name3-20000101-3ghj.json"],
        [4, "Name3", "/private/var", "name3-20000101-klmn.json"],
        [52, "NewName2", "/lost+found", "newname2-20000101-p123.json"],
    ]


def test_function_does_not_duplicate_data(conn):
    load_design("tests/parquet_test_files/test-design.parquet", conn)
    load_design("tests/parquet_test_files/test-design.parquet", conn)
    load_design("tests/parquet_test_files/test-design-update.parquet", conn)
    load_design("tests/parquet_test_files/test-design-updates.parquet", conn)
    load_design("tests/parquet_test_files/test-design-updates.parquet", conn)
    result = conn.run("SELECT * FROM dim_design;")
    assert len(result) == 7
