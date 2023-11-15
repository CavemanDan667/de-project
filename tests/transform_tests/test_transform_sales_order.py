from src.transform.transform_utils.transform_sales_order import (
    transform_sales_order
)
from pytest import raises
import pandas as pd


def test_function_returns_data_frame():
    sales_result = transform_sales_order(
        's3://de-project-test-data/csv/test-sales-order.csv'
    )
    assert isinstance(sales_result, pd.core.frame.DataFrame)


def test_function_returns_correct_data():
    sales_order_result = transform_sales_order(
        's3://de-project-test-data/csv/test-sales-order.csv'
    )
    sales_order_list = sales_order_result.values.tolist()
    assert sales_order_list[0] == [
        1, '2022-01-01', '10:00:00',
        '2023-10-10', '11:30:00',
        1, 3, 12345, 1.25, 3, 4,
        '2022-11-09', '2022-11-07', 4
    ]


def test_function_raises_error_with_incorrect_csv():
    with raises(ValueError):
        transform_sales_order(
            's3://de-project-test-data/csv/test-currency.csv'
        )
