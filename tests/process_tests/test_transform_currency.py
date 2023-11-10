from src.process.process_utils.transform_currency import transform_currency
from pytest import raises
import pandas as pd


def test_function_returns_data_frame():
    result = transform_currency(
        's3://de-project-test-data/csv/test-currency.csv'
        )
    assert isinstance(result, pd.core.frame.DataFrame)


def test_function_returns_correct_data():
    result = transform_currency(
       's3://de-project-test-data/csv/test-currency.csv'
    )
    assert result.values.tolist() == [
        [1, 'GBP', 'British pound'],
        [2, 'USD', 'United States dollar'],
        [3, 'EUR', 'European Euro']
    ]


def test_function_raises_value_error_with_incorrect_data():
    with raises(ValueError):
        transform_currency(
            's3://de-project-test-data/csv/test-staff.csv'
        )


def test_function_names_unknown_currency_as_None():
    result = transform_currency(
       's3://de-project-test-data/csv/test-fake-currency.csv'
    )
    assert result.values.tolist() == [
        [6, 'XYZ', None]
    ]
