from src.process.process_utils.transform_payment import (
    transform_payment
)
from pytest import raises
import pandas as pd


def test_function_returns_data_frame():
    payment_result = transform_payment(
        's3://de-project-test-data/csv/test-payment.csv'
    )
    assert isinstance(payment_result, pd.core.frame.DataFrame)


def test_function_returns_correct_data():
    payment_result = transform_payment(
        's3://de-project-test-data/csv/test-payment.csv'
    )
    payment_list = payment_result.values.tolist()
    assert payment_list[0] == [1, '2020-01-01', '10:00:00',
                               '2020-10-10', '11:30:00', 1,
                               1, 123.45, 1, 1, False,
                               '2023-10-10']


def test_function_raises_value_error_with_incorrect_csv():
    with raises(ValueError):
        transform_payment(
            's3://de-project-test-data/csv/test-currency.csv'
        )
