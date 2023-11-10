from src.process.process_utils.transform_address import transform_address
import pandas as pd


def test_function_returns_data_frame():
    result = transform_address(
        "s3://de-project-test-data/csv/test-address.csv")
    assert isinstance(result, pd.core.frame.DataFrame)


def test_function_returns_correct_data():
    result = transform_address(
        "s3://de-project-test-data/csv/test-address.csv")
    assert result.values.tolist()[3] == [
        4,
        "5 Far Lane",
        "Parkway",
        "North Shore",
        "Castletown",
        "AB2 3CD",
        "Wales",
        "1234 800900",
    ]
