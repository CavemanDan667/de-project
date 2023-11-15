from src.transform.transform_utils.transform_purchase_order import (
    transform_purchase_order
)
from pytest import raises
import pandas as pd


def test_function_returns_data_frame():
    purchase_order_result = transform_purchase_order(
        's3://de-project-test-data/csv/test-purchase-order.csv'
    )
    assert isinstance(purchase_order_result, pd.core.frame.DataFrame)


def test_function_returns_correct_data():
    purchase_order_result = transform_purchase_order(
        's3://de-project-test-data/csv/test-purchase-order.csv'
    )
    purchase_order_list = purchase_order_result.values.tolist()
    assert purchase_order_list[0] == [1, '2000-01-01', '10:00:00',
                                      '2020-10-10', '11:30:00', 1, 3,
                                      'AA2AA2A', 123, 100.5, 2,
                                      '2022-11-09', '2022-11-07', 1]


def test_function_raises_value_error_with_incorrect_csv():
    with raises(ValueError):
        transform_purchase_order(
            's3://de-project-test-data/csv/test-currency.csv'
        )
