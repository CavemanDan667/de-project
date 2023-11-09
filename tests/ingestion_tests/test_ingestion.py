# from ingestion.ingestion import handler
from pg8000.native import Connection
from tests.get_credentials import get_credentials
import pytest
# import unittest
config = get_credentials('test_totesys_db_creds')

user = config["TEST_USER"]
password = config["TEST_PASSWORD"]
host = config["TEST_HOST"]
port = config["TEST_PORT"]
database = config["TEST_DATABASE"]


@pytest.fixture
def conn():
    return Connection(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database)


def test_valid_database_connection():
    pass
    # with unittest.TestCase().assertLogs() as logs:
    #     handler(event=None, context=None)
    #     assert 'CREATED' in logs.output[0]
