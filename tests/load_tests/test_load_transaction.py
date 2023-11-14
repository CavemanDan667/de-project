from src.loading.load_utils.load_transaction import load_transaction
from src.loading.load_utils.load_sales_order import load_sales_order
from src.loading.load_utils.load_purchase_order import load_purchase_order
from src.loading.load_utils.load_design import load_design
from src.loading.load_utils.load_address import load_address
from src.loading.load_utils.load_counterparty import load_counterparty
from src.loading.load_utils.load_currency import load_currency
from src.loading.load_utils.load_staff import load_staff
from src.loading.load_utils.get_credentials import get_credentials
from pg8000.native import Connection
from dotenv import dotenv_values
import pytest
import subprocess
from unittest.mock import MagicMock

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
        user=user,
        password=password,
        host=host,
        port=port,
        database=database
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
    load_sales_order(
        "s3://de-project-test-data/parquet/test-sales-order.parquet",
        conn
    )
    load_purchase_order(
        's3://de-project-test-data/parquet/test-purchase-order.parquet',
        conn
    )
    result = load_transaction(
        "s3://de-project-test-data/parquet/test-transaction.parquet",
        conn
    )

    assert result == "Data loaded successfully - dim_transaction"


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
        "s3://de-project-test-data/parquet/test-sales-order.parquet",
        conn
    )
    load_purchase_order(
        's3://de-project-test-data/parquet/test-purchase-order.parquet',
        conn
    )
    load_transaction(
        "s3://de-project-test-data/parquet/test-transaction.parquet",
        conn
    )
    result = conn.run("SELECT * FROM dim_transaction;")
    assert result == [[1, 'PURCHASE', None, 2],
                      [2, 'PURCHASE', None, 3],
                      [3, 'SALE', 8, None],
                      [4, 'PURCHASE', None, 1],
                      [5, 'PURCHASE', None, 4],
                      [6, 'SALE', 2, None],
                      [7, 'SALE', 3, None],
                      [8, 'PURCHASE', None, 5],
                      [9, 'SALE', 5, None],
                      [10, 'SALE', 4, None]]


def test_function_does_not_duplicate_data(conn):
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
        "s3://de-project-test-data/parquet/test-sales-order.parquet",
        conn
    )
    load_purchase_order(
        's3://de-project-test-data/parquet/test-purchase-order.parquet',
        conn
    )
    load_transaction(
        "s3://de-project-test-data/parquet/test-transaction.parquet",
        conn
    )
    load_transaction(
        "s3://de-project-test-data/parquet/test-transaction.parquet",
        conn
    )
    load_transaction(
        "s3://de-project-test-data/parquet/test-transaction.parquet",
        conn
    )
    load_transaction(
        "s3://de-project-test-data/parquet/test-transaction.parquet",
        conn
    )
    result = conn.run("SELECT * FROM dim_transaction;")
    assert result == [[1, 'PURCHASE', None, 2],
                      [2, 'PURCHASE', None, 3],
                      [3, 'SALE', 8, None],
                      [4, 'PURCHASE', None, 1],
                      [5, 'PURCHASE', None, 4],
                      [6, 'SALE', 2, None],
                      [7, 'SALE', 3, None],
                      [8, 'PURCHASE', None, 5],
                      [9, 'SALE', 5, None],
                      [10, 'SALE', 4, None]]


def test_function_raises_error_with_incorrect_parquet_file(conn, caplog):
    with pytest.raises(KeyError):
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
            "s3://de-project-test-data/parquet/test-sales-order.parquet",
            conn
        )
        load_purchase_order(
            's3://de-project-test-data/parquet/test-purchase-order.parquet',
            conn
        )
        load_transaction(
            's3://de-project-test-data/parquet/test-payment-type.parquet',
            conn
        )
    assert "load_transaction was given an incorrect file" in caplog.text


def test_function_raises_error_on_null_data(conn):
    with pytest.raises(TypeError):
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
            "s3://de-project-test-data/parquet/test-sales-order.parquet",
            conn
        )
        load_purchase_order(
            's3://de-project-test-data/parquet/test-purchase-order.parquet',
            conn
        )
        load_transaction(
            's3://de-project-test-data/parquet/test-fake-transaction.parquet',
            conn
        )


def test_function_calls_conn_with_correct_SQL_query():
    mock_conn = MagicMock()
    load_transaction(
        "s3://de-project-test-data/parquet/test-transaction.parquet",
        mock_conn
        )
    expected_insert_query_list = [
        "INSERT INTO dim_transaction",
        "(transaction_id, transaction_type,",
        "sales_order_id, purchase_order_id)",
        "VALUES",
        ]
    assert mock_conn.run.call_count == 20
    assert expected_insert_query_list[0] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[1] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[2] in str(mock_conn.run.call_args)
    assert expected_insert_query_list[3] in str(mock_conn.run.call_args)
