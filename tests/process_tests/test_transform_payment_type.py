from src.process.process_utils.transform_payment_type import (
    transform_payment_type)
import pandas as pd
from pytest import raises


def test_function_returns_data_frame():
    result = transform_payment_type(
        "s3://de-project-test-data/csv/test-payment-type.csv"
    )
    assert isinstance(result, pd.core.frame.DataFrame)


def test_function_returns_correct_data():
    result = transform_payment_type(
        "s3://de-project-test-data/csv/test-payment-type.csv"
    )
    assert result.values.tolist() == [
        [1, "TYPE_ONE"],
        [2, "TYPE_TWO"],
        [3, "TYPE_THREE"],
        [4, "TYPE_FOUR"],
    ]


def test_function_only_returns_new_data():
    result = transform_payment_type(
        "s3://de-project-test-data/csv/test-payment-type-plus.csv"
    )
    assert result.values.tolist() == [[5, "TYPE_FIVE"]]


def test_function_raises_value_error_with_wrong_data():
    with raises(ValueError):
        transform_payment_type(
            "s3://de-project-test-data/csv/test-currency.csv")
