from src.process.process_utils.transform_transaction import (
    transform_transaction
)
import pandas as pd
import numpy as np
import pytest


def test_function_returns_data_frame():
    result = transform_transaction(
        "s3://de-project-test-data/csv/test-transaction.csv"
    )
    assert isinstance(result, pd.core.frame.DataFrame)


def test_function_returns_correct_data():
    result = transform_transaction(
        "s3://de-project-test-data/csv/test-transaction.csv"
    )
    result = result.replace({np.nan: None})
    assert result.values.tolist() == [
        [1, 'PURCHASE', None, 2],
        [2, 'PURCHASE', None, 3],
        [3, 'SALE', 8, None],
        [4, 'PURCHASE', None, 1],
        [5, 'PURCHASE', None, 4],
        [6, 'SALE', 2, None],
        [7, 'SALE', 3, None],
        [8, 'PURCHASE', None, 5],
        [9, 'SALE', 5, None],
        [10, 'SALE', 4, None]]


def test_function_raises_value_error_with_wrong_file():
    with pytest.raises(ValueError):
        transform_transaction(
            "s3://de-project-test-data/csv/test-currency.csv"
        )
