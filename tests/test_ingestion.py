# from ingestion.ingestion import handler
from pg8000.native import Connection

from dotenv import dotenv_values
import pytest
# import unittest
config = dotenv_values(".env")

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
