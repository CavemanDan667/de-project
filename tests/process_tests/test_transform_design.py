from src.process.process_utils.transform_design import transform_design
import pandas as pd
import pytest


def test_function_returns_data_frame():
    result = transform_design("s3://de-project-test-data/csv/test-design.csv")
    assert isinstance(result, pd.core.frame.DataFrame)


def test_function_returns_correct_data():
    result = transform_design("s3://de-project-test-data/csv/test-design.csv")
    assert result.values.tolist() == [
        [18, "Name1", "/usr", "name1-20000101-abcd.json"],
        [29, "Name2", "/private", "name2-20000101-4eff.json"],
        [345, "Name3", "/private/var", "name3-20000101-3ghj.json"],
        [4, "Name3", "/private/var", "name3-20000101-klmn.json"],
        [52, "Name2", "/lost+found", "name2-20000101-p123.json"],
    ]


def test_function_raises_value_error_with_incorrect_file():
    with pytest.raises(ValueError):
        transform_design(
            "s3://de-project-test-data/csv/test-currency.csv"
        )
