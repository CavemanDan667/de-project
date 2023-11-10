from src.process.process_utils.transform_payment_type import (
    transform_payment_type
)
import pandas as pd


def test_function_returns_data_frame():
    result = transform_payment_type(
        'tests/csv_test_files/test-payment-type.csv'
        )
    assert isinstance(result, pd.core.frame.DataFrame)


def test_function_returns_correct_data():
    result = transform_payment_type(
       'tests/csv_test_files/test-payment-type.csv'
    )
    assert result.values.tolist() == [
        [1, 'TYPE_ONE'],
        [2, 'TYPE_TWO'],
        [3, 'TYPE_THREE'],
        [4, 'TYPE_FOUR']
    ]


def test_function_only_returns_new_data():
    result = transform_payment_type(
       'tests/csv_test_files/test-payment-type-plus.csv'
    )
    assert result.values.tolist() == [
        [5, 'TYPE_FIVE']
    ]

# test for key error
