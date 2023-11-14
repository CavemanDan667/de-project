from src.loading.load_utils.load_sales_order import load_sales_order
from src.loading.load_utils.load_address import load_address
from src.loading.load_utils.load_counterparty import load_counterparty
from src.loading.load_utils.load_currency import load_currency
from src.loading.load_utils.load_design import load_design
from src.loading.load_utils.load_staff import load_staff
from src.loading.load_utils.get_credentials import get_credentials
from pg8000.native import Connection, DatabaseError
from dotenv import dotenv_values
import pytest
import subprocess
import datetime
from decimal import Decimal

identity = subprocess.check_output("whoami")

if identity == b"runner\n":
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
    load_design(
       's3://de-project-test-data/parquet/test-design.parquet',
       conn
    )
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
    result = load_sales_order(
        "s3://de-project-test-data/parquet/test-sales-order.parquet", conn)
    assert result == "Data loaded successfully - fact_sales_order"


def test_function_correctly_populates_table(conn):
    load_design(
       's3://de-project-test-data/parquet/test-design.parquet',
       conn
    )
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
    load_sales_order(
        's3://de-project-test-data/parquet/test-sales-order.parquet',
        conn
    )
    sales_order_result = conn.run('SELECT * FROM fact_sales_order')
    assert sales_order_result[0:1] == [
        [1, 1, datetime.date(2022, 1, 1),
         datetime.time(10, 0), datetime.date(2023, 10, 10),
         datetime.time(11, 30), 1, 3, 12345, Decimal(1.25), 3, 4,
         datetime.date(2022, 11, 9), datetime.date(2022, 11, 7), 4]]


def test_function_does_not_repeat_duplicate_data(conn):
    load_design(
       's3://de-project-test-data/parquet/test-design.parquet',
       conn
    )
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
    load_sales_order(
        's3://de-project-test-data/parquet/test-sales-order.parquet',
        conn
    )
    load_sales_order(
        's3://de-project-test-data/parquet/test-sales-order.parquet',
        conn
    )
    sales_order_result = conn.run(
        'SELECT * FROM fact_sales_order WHERE sales_order_id = 2;'
    )
    assert len(sales_order_result) == 1


def test_function_adds_updated_data_to_table(conn):
    load_sales_order(
        's3://de-project-test-data/parquet/test-sales-order-update.parquet',
        conn
    )
    sales_order_full_table = conn.run('SELECT * FROM fact_sales_order;')
    assert sales_order_full_table == [
        [1, 1, datetime.date(2022, 1, 1), datetime.time(10, 0),
         datetime.date(2023, 10, 10), datetime.time(11, 30),
         1, 3, 12345, Decimal(1.25), 3, 4, datetime.date(2022, 11, 9),
         datetime.date(2022, 11, 7), 4],
        [2, 2, datetime.date(2022, 1, 1),
         datetime.time(10, 0), datetime.date(2023, 10, 10),
         datetime.time(11, 30), 1, 2, 20000, Decimal(2.25), 2, 29,
         datetime.date(2022, 11, 9), datetime.date(2022, 11, 7), 2],
        [3, 3, datetime.date(2022, 1, 1), datetime.time(10, 0),
         datetime.date(2023, 10, 10), datetime.time(11, 30),
         2, 4, 30000, Decimal(3.25), 3, 18, datetime.date(2022, 11, 9),
         datetime.date(2022, 11, 7), 3],
        [4, 4, datetime.date(2022, 1, 1), datetime.time(10, 0),
         datetime.date(2023, 10, 10), datetime.time(11, 30),
         3, 1, 40000, Decimal(4.25), 2, 345, datetime.date(2022, 11, 9),
         datetime.date(2022, 11, 7), 4],
        [5, 5, datetime.date(2022, 1, 1), datetime.time(10, 0),
         datetime.date(2023, 10, 10), datetime.time(11, 30),
         2, 4, 10000, Decimal(5.25), 3, 52, datetime.date(2022, 11, 9),
         datetime.date(2022, 11, 7), 1],
        [6, 6, datetime.date(2022, 1, 1), datetime.time(10, 0),
         datetime.date(2023, 10, 10), datetime.time(11, 30),
         5, 1, 54321, Decimal(6.25), 3, 52, datetime.date(2022, 11, 9),
         datetime.date(2022, 11, 7), 5],
        [7, 7, datetime.date(2022, 1, 1), datetime.time(10, 0),
         datetime.date(2023, 10, 10), datetime.time(11, 30),
         5, 2, 60000, Decimal(7.25), 2, 345, datetime.date(2022, 11, 9),
         datetime.date(2022, 11, 7), 2],
        [8, 8, datetime.date(2022, 1, 1), datetime.time(10, 0),
         datetime.date(2023, 10, 10), datetime.time(11, 30),
         1, 3, 700, Decimal(8.25), 2, 4, datetime.date(2022, 11, 9),
         datetime.date(2022, 11, 7), 3],
        [9, 1, datetime.date(2022, 1, 1), datetime.time(10, 0),
         datetime.date(2023, 10, 20), datetime.time(12, 30),
         1, 3, 12345, Decimal(1.25), 3, 4, datetime.date(2022, 11, 10),
         datetime.date(2022, 11, 7), 4]]


def test_function_raises_database_error_with_incorrect_data(conn):
    with pytest.raises(DatabaseError):
        load_sales_order(
            "s3://de-project-test-data/parquet/test-purchase-order.parquet",
            conn
        )


def test_function_raises_key_error_with_incorrect_data(conn):
    with pytest.raises(KeyError):
        load_sales_order(
            "s3://de-project-test-data/parquet/test-address.parquet",
            conn
        )


def test_function_raises_index_error_with_incorrect_data(conn):
    with pytest.raises(IndexError):
        load_sales_order(
            "s3://de-project-test-data/parquet/test-currency.parquet",
            conn
        )
