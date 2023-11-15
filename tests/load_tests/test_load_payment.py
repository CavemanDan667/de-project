from src.loading.load_utils.load_payment import load_payment
from src.loading.load_utils.load_purchase_order import load_purchase_order
from src.loading.load_utils.load_sales_order import load_sales_order
from src.loading.load_utils.load_address import load_address
from src.loading.load_utils.load_design import load_design
from src.loading.load_utils.load_counterparty import load_counterparty
from src.loading.load_utils.load_currency import load_currency
from src.loading.load_utils.load_staff import load_staff
from src.loading.load_utils.load_payment_type import load_payment_type
from src.loading.load_utils.load_transaction import load_transaction
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
    load_purchase_order(
        "s3://de-project-test-data/parquet/test-purchase-order.parquet",
        conn
    )
    load_payment_type(
        "s3://de-project-test-data/parquet/test-payment-type.parquet",
        conn
    )
    load_sales_order(
        "s3://de-project-test-data/parquet/test-sales-order.parquet",
        conn
    )
    load_transaction(
        "s3://de-project-test-data/parquet/test-transaction.parquet",
        conn
    )
    result = load_payment(
        "s3://de-project-test-data/parquet/test-payment.parquet",
        conn
    )
    assert result == "Data loaded successfully - fact_payment"


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
    load_purchase_order(
        "s3://de-project-test-data/parquet/test-purchase-order.parquet",
        conn
    )
    load_payment_type(
        "s3://de-project-test-data/parquet/test-payment-type.parquet",
        conn
    )
    load_sales_order(
        "s3://de-project-test-data/parquet/test-sales-order.parquet",
        conn
    )
    load_transaction(
        "s3://de-project-test-data/parquet/test-transaction.parquet",
        conn
    )
    load_payment(
        "s3://de-project-test-data/parquet/test-payment.parquet",
        conn
    )
    payment_result = conn.run('SELECT * FROM fact_payment;')
    assert payment_result[0:3] == [
        [1, 1, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2020, 10, 10), datetime.time(11, 30), 1, 1,
         Decimal('123.45'), 1, 1, False, datetime.date(2023, 10, 10)],
        [2, 2, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2020, 10, 10), datetime.time(11, 30), 2, 2,
         Decimal('3333.55'), 2, 3, True, datetime.date(2023, 10, 10)],
        [3, 3, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2020, 10, 10), datetime.time(11, 30), 3, 3,
         Decimal('60500.99'), 3, 1, False, datetime.date(2023, 10, 10)]]


def test_function_does_not_repeat_duplicate_data(conn):
    # ensure the reference tables are populated first
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
    load_purchase_order(
        "s3://de-project-test-data/parquet/test-purchase-order.parquet",
        conn
    )
    load_payment_type(
        "s3://de-project-test-data/parquet/test-payment-type.parquet",
        conn
    )
    load_sales_order(
        "s3://de-project-test-data/parquet/test-sales-order.parquet",
        conn
    )
    load_transaction(
        "s3://de-project-test-data/parquet/test-transaction.parquet",
        conn
    )
    # run the load command multiple times with the same data
    load_payment(
        "s3://de-project-test-data/parquet/test-payment.parquet",
        conn
    )
    load_payment(
        "s3://de-project-test-data/parquet/test-payment.parquet",
        conn
    )
    load_payment(
        "s3://de-project-test-data/parquet/test-payment.parquet",
        conn
    )
    # assert that there is only one entry in the table per ID
    payment_result = conn.run(
        'SELECT * FROM fact_payment WHERE payment_id = 3;'
    )
    assert len(payment_result) == 1


def test_function_adds_additional_data_to_table(conn):
    load_payment(
        's3://de-project-test-data/parquet/test-payment-update.parquet',
        conn
    )
    purchase_order_full_table = conn.run('SELECT * FROM fact_payment;')
    assert purchase_order_full_table == [
        [1, 1, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2020, 10, 10), datetime.time(11, 30),
         1, 1, Decimal('123.45'), 1, 1, False, datetime.date(2023, 10, 10)],
        [2, 2, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2020, 10, 10), datetime.time(11, 30),
         2, 2, Decimal('3333.55'), 2, 3, True, datetime.date(2023, 10, 10)],
        [3, 3, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2020, 10, 10), datetime.time(11, 30),
         3, 3, Decimal('60500.99'), 3, 1, False, datetime.date(2023, 10, 10)],
        [4, 4, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2020, 10, 10), datetime.time(11, 30),
         1, 3, Decimal('34564.23'), 2, 3, False, datetime.date(2023, 10, 10)]]


def test_function_adds_updated_data_to_table(conn):
    load_payment(
        's3://de-project-test-data/parquet/test-payment-update2.parquet',
        conn
    )
    purchase_order_full_table = conn.run('SELECT * FROM fact_payment;')
    assert purchase_order_full_table == [
        [1, 1, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2020, 10, 10), datetime.time(11, 30),
         1, 1, Decimal('123.45'), 1, 1, False, datetime.date(2023, 10, 10)],
        [2, 2, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2020, 10, 10), datetime.time(11, 30),
         2, 2, Decimal('3333.55'), 2, 3, True, datetime.date(2023, 10, 10)],
        [3, 3, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2020, 10, 10), datetime.time(11, 30),
         3, 3, Decimal('60500.99'), 3, 1, False, datetime.date(2023, 10, 10)],
        [4, 4, datetime.date(2020, 1, 1), datetime.time(10, 0),
         datetime.date(2020, 10, 10), datetime.time(11, 30),
         1, 3, Decimal('34564.23'), 2, 3, True, datetime.date(2023, 10, 10)]]


def test_function_returns_index_error_with_incorrect_data(conn):
    with pytest.raises(IndexError):
        load_payment(
            "s3://de-project-test-data/parquet/test-currency.parquet",
            conn
        )


def test_function_returns_key_error_with_incorrect_null_data(conn):
    with pytest.raises(KeyError):
        load_payment(
            "s3://de-project-test-data/parquet/test-address.parquet",
            conn
        )
