from src.process.process_utils.transform_sales_order import transform_sales_order # noqa
from src.process.process_utils.transform_design import transform_design
from src.process.process_utils.transform_staff import transform_staff
from src.process.process_utils.transform_department import transform_department
from src.process.process_utils.transform_address import transform_address
from src.process.process_utils.transform_counterparty import transform_counterparty # noqa
from src.process.process_utils.transform_currency import transform_currency
import datetime


from pg8000.native import Connection
from dotenv import dotenv_values
import pytest
import pandas as pd

config = dotenv_values(".env")

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


def test_function_returns_data_frame(conn):
    transform_design(
       'tests/csv_test_files/test-design.csv',
       conn
    )
    transform_department(
        'tests/csv_test_files/test-department.csv',
        conn
        )
    transform_staff(
       'tests/csv_test_files/test-staff.csv',
       conn
    )
    transform_address(
       'tests/csv_test_files/test-address.csv',
       conn
    )
    transform_counterparty(
        'tests/csv_test_files/test-counterparty.csv',
        conn
    )
    transform_currency(
        'tests/csv_test_files/test-currency.csv',
        conn
        )
    sales_result = transform_sales_order(
        'tests/csv_test_files/test-sales-order.csv',
        conn
    )
    assert isinstance(sales_result, pd.core.frame.DataFrame)


def test_function_returns_correct_data(conn):
    transform_design(
       'tests/csv_test_files/test-design.csv',
       conn
    )
    transform_department(
        'tests/csv_test_files/test-department.csv',
        conn
        )
    transform_staff(
       'tests/csv_test_files/test-staff.csv',
       conn
    )
    transform_address(
       'tests/csv_test_files/test-address.csv',
       conn
    )
    transform_counterparty(
        'tests/csv_test_files/test-counterparty.csv',
        conn
    )
    transform_currency(
        'tests/csv_test_files/test-currency.csv',
        conn
        )
    sales_result = transform_sales_order(
        'tests/csv_test_files/test-sales-order.csv',
        conn
    )
    assert sales_result.values.tolist()[0:1] == [
        [1, 1, datetime.date(2022, 1, 1),
         datetime.time(10, 0), datetime.date(2023, 10, 10),
         datetime.time(11, 30), 1, 3, 12345, 1.25, 3, 4,
         datetime.date(2022, 11, 9), datetime.date(2022, 11, 7), 4]]


def test_function_correctly_populates_table(conn):
    transform_design(
       'tests/csv_test_files/test-design.csv',
       conn
    )
    transform_department(
        'tests/csv_test_files/test-department.csv',
        conn
        )
    transform_staff(
       'tests/csv_test_files/test-staff.csv',
       conn
    )
    transform_address(
       'tests/csv_test_files/test-address.csv',
       conn
    )
    transform_counterparty(
        'tests/csv_test_files/test-counterparty.csv',
        conn
    )
    transform_currency(
        'tests/csv_test_files/test-currency.csv',
        conn
        )
    transform_sales_order(
        'tests/csv_test_files/test-sales-order.csv',
        conn
    )
    sales_order_result = conn.run('SELECT * FROM fact_sales_order')
    assert sales_order_result[0:1] == [
        [1, 1, datetime.date(2022, 1, 1),
         datetime.time(10, 0), datetime.date(2023, 10, 10),
         datetime.time(11, 30), 1, 3, 12345, 1.25, 3, 4,
         datetime.date(2022, 11, 9), datetime.date(2022, 11, 7), 4]]


def test_function_does_not_repeat_duplicate_data(conn):
    transform_design(
       'tests/csv_test_files/test-design.csv',
       conn
    )
    transform_department(
        'tests/csv_test_files/test-department.csv',
        conn
        )
    transform_staff(
       'tests/csv_test_files/test-staff.csv',
       conn
    )
    transform_address(
       'tests/csv_test_files/test-address.csv',
       conn
    )
    transform_counterparty(
        'tests/csv_test_files/test-counterparty.csv',
        conn
    )
    transform_currency(
        'tests/csv_test_files/test-currency.csv',
        conn
        )
    transform_sales_order(
        'tests/csv_test_files/test-sales-order.csv',
        conn
    )
    sales_order_result = conn.run('SELECT * FROM fact_sales_order;')
    assert len(sales_order_result) == 8


def test_function_correctly_updates_data(conn):
    transform_design(
       'tests/csv_test_files/test-design.csv',
       conn
    )
    transform_department(
        'tests/csv_test_files/test-department.csv',
        conn
        )
    transform_staff(
       'tests/csv_test_files/test-staff.csv',
       conn
    )
    transform_address(
       'tests/csv_test_files/test-address.csv',
       conn
    )
    transform_counterparty(
        'tests/csv_test_files/test-counterparty.csv',
        conn
    )
    transform_currency(
        'tests/csv_test_files/test-currency.csv',
        conn
    )
    transform_sales_order(
        'tests/csv_test_files/test-sales-order-update.csv',
        conn
    )
    sales_order_result = conn.run('SELECT * FROM fact_sales_order')
    assert sales_order_result == [
        [2, 2, datetime.date(2022, 1, 1),
         datetime.time(10, 0), datetime.date(2023, 10, 10),
         datetime.time(11, 30), 1, 2, 20000, 2.25, 2, 29,
         datetime.date(2022, 11, 9), datetime.date(2022, 11, 7), 2],
        [3, 3, datetime.date(2022, 1, 1), datetime.time(10, 0),
         datetime.date(2023, 10, 10), datetime.time(11, 30),
         2, 4, 30000, 3.25, 3, 18, datetime.date(2022, 11, 9),
         datetime.date(2022, 11, 7), 3],
        [4, 4, datetime.date(2022, 1, 1), datetime.time(10, 0),
         datetime.date(2023, 10, 10), datetime.time(11, 30),
         3, 1, 40000, 4.25, 2, 345, datetime.date(2022, 11, 9),
         datetime.date(2022, 11, 7), 4],
        [5, 5, datetime.date(2022, 1, 1), datetime.time(10, 0),
         datetime.date(2023, 10, 10), datetime.time(11, 30),
         2, 4, 10000, 5.25, 3, 52, datetime.date(2022, 11, 9),
         datetime.date(2022, 11, 7), 1],
        [6, 6, datetime.date(2022, 1, 1), datetime.time(10, 0),
         datetime.date(2023, 10, 10), datetime.time(11, 30),
         5, 1, 54321, 6.25, 3, 52, datetime.date(2022, 11, 9),
         datetime.date(2022, 11, 7), 5],
        [7, 7, datetime.date(2022, 1, 1), datetime.time(10, 0),
         datetime.date(2023, 10, 10), datetime.time(11, 30),
         5, 2, 60000, 7.25, 2, 345, datetime.date(2022, 11, 9),
         datetime.date(2022, 11, 7), 2],
        [8, 8, datetime.date(2022, 1, 1), datetime.time(10, 0),
         datetime.date(2023, 10, 10), datetime.time(11, 30),
         1, 3, 700, 8.25, 2, 4, datetime.date(2022, 11, 9),
         datetime.date(2022, 11, 7), 3],
        [1, 1, datetime.date(2022, 1, 1), datetime.time(10, 0),
         datetime.date(2023, 10, 10), datetime.time(11, 30),
         1, 3, 12345, 1.25, 3, 52, datetime.date(2022, 11, 9),
         datetime.date(2022, 11, 7), 4]]
