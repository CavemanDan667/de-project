from src.loading.load_utils.load_purchase_order import load_purchase_order
from src.loading.load_utils.load_address import load_address
from src.loading.load_utils.load_counterparty import load_counterparty
from src.loading.load_utils.load_currency import load_currency
from src.loading.load_utils.load_staff import load_staff
from src.loading.load_utils.get_credentials import get_credentials
from pg8000.native import Connection
from dotenv import dotenv_values
import pytest
import subprocess
import datetime
from decimal import Decimal

identity = subprocess.check_output("whoami")

if identity == b'runner\n':
    config = get_credentials("test_dw_creds")
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
    load_staff(
       's3://de-project-test-data/parquet/staff-test.parquet',
       conn
    )
    load_address(
       's3://de-project-test-data/parquet/test-address.parquet',
       conn
    )
    load_counterparty(
        's3://de-project-test-data/parquet/test-counterparty.parquet',
        conn
    )
    load_currency(
        's3://de-project-test-data/parquet/test-currency.parquet',
        conn
    )
    result = load_purchase_order(
        "s3://de-project-test-data/parquet/test-purchase-order.parquet", conn)
    assert result == "Data loaded successfully - fact_purchase_order"


def test_function_correctly_populates_table(conn):
    load_staff(
       's3://de-project-test-data/parquet/staff-test.parquet',
       conn
    )
    load_address(
       's3://de-project-test-data/parquet/test-address.parquet',
       conn
    )
    load_counterparty(
        's3://de-project-test-data/parquet/test-counterparty.parquet',
        conn
    )
    load_currency(
        's3://de-project-test-data/parquet/test-currency.parquet',
        conn
        )
    load_purchase_order(
        's3://de-project-test-data/parquet/test-purchase-order.parquet',
        conn
    )
    purchase_order_result = conn.run('SELECT * FROM fact_purchase_order;')
    assert purchase_order_result[0:1] == [
        [1, 1, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2021, 10, 10), datetime.time(11, 30), 1, 3,
         'AA2AA2A', 123, 100.5, 2, datetime.date(2022, 11, 7),
         datetime.date(2022, 11, 9), 1]]


def test_function_does_not_repeat_duplicate_data(conn):
    # ensure the reference tables are populated first
    load_staff(
       's3://de-project-test-data/parquet/staff-test.parquet',
       conn
    )
    load_address(
       's3://de-project-test-data/parquet/test-address.parquet',
       conn
    )
    load_counterparty(
        's3://de-project-test-data/parquet/test-counterparty.parquet',
        conn
    )
    load_currency(
        's3://de-project-test-data/parquet/test-currency.parquet',
        conn
    )
    # run the load command multiple times with the same data
    load_purchase_order(
        's3://de-project-test-data/parquet/test-purchase-order.parquet',
        conn
    )
    load_purchase_order(
        's3://de-project-test-data/parquet/test-purchase-order.parquet',
        conn
    )
    load_purchase_order(
        's3://de-project-test-data/parquet/test-purchase-order.parquet',
        conn
    )
    # assert that there is only one entry in the table per ID
    purchase_order_result = conn.run(
        'SELECT * FROM fact_purchase_order WHERE purchase_order_id = 3;'
    )
    assert len(purchase_order_result) == 1


def test_function_adds_updated_data_to_table(conn):
    load_purchase_order(
        's3://de-project-test-data/parquet/test-purchase-order-update.parquet',
        conn
    )
    purchase_order_full_table = conn.run('SELECT * FROM fact_purchase_order;')
    assert purchase_order_full_table == [
        [1, 1, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2021, 10, 10), datetime.time(11, 30), 1, 3,
         'AA2AA2A', 123, Decimal('100.5'), 2,
         datetime.date(2022, 11, 7), datetime.date(2022, 11, 9), 1],
        [2, 2, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2021, 10, 10), datetime.time(11, 30), 3, 2,
         'BBBB1BB', 45, Decimal('200.0'), 2,
         datetime.date(2022, 11, 7), datetime.date(2022, 11, 9), 2],
        [3, 3, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2021, 10, 10), datetime.time(11, 30), 3, 4,
         'CC9CCC9', 6700, Decimal('3.1'), 2,
         datetime.date(2022, 11, 7), datetime.date(2022, 11, 9), 3],
        [4, 4, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2021, 10, 10), datetime.time(11, 30), 2, 2,
         'DD2DDD2', 8, Decimal('300.25'), 3,
         datetime.date(2022, 11, 7), datetime.date(2022, 11, 9), 4],
        [5, 5, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2021, 10, 10), datetime.time(11, 30), 4, 1,
         'EEEE7EE', 100, Decimal('25.25'), 2,
         datetime.date(2022, 11, 7), datetime.date(2022, 11, 9), 5],
        [6, 6, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2021, 10, 10), datetime.time(11, 30), 5, 3,
         'FFF7FFF', 1000, Decimal('10.0'), 1,
         datetime.date(2022, 11, 7), datetime.date(2022, 11, 9), 5],
        [7, 7, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2021, 10, 10), datetime.time(11, 30), 1, 3,
         'GGG33GG', 20, Decimal('123.45'), 2,
         datetime.date(2022, 11, 7), datetime.date(2022, 11, 9), 5],
        [8, 1, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2022, 10, 10), datetime.time(11, 30), 1, 3,
         'AA2AA2A', 123, Decimal('100.5'), 2,
         datetime.date(2022, 11, 7), datetime.date(2022, 11, 9), 1]]
