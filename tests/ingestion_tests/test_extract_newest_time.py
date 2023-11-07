from src.ingestion.ingestion_utils.extract_newest_time import (
    extract_newest_time
)
import pytest


def test_function_returns_default_of_0_if_passed_empty_list():
    result = extract_newest_time([])
    assert result == 0


def test_function_extracts_timestamp_as_int_given_one_file():
    result = extract_newest_time(["file/12345.csv"])
    assert result == 12345


def test_function_returns_largest_numbers_when_multiple_files():
    test_files = ["file/1.csv", "file/2.csv", "file/4.csv", "file/3.csv"]
    result = extract_newest_time(test_files)
    assert result == 4


def test_function_returns_largest_numbers_when_varied_names():
    test_files = ["apple/20.csv", "banana/50.csv", "cherry/25.csv"]
    result = extract_newest_time(test_files)
    assert result == 50


def test_function_returns_largest_number_when_lengths_vary():
    test_files = [
        "apple/153563.csv",
        "banana/321321.csv",
        "cherry/8836.csv",
        "damson/299.csv",
    ]
    result = extract_newest_time(test_files)
    assert result == 321321


def test_function_raises_error_if_passed_None_variable():
    test_var = None
    with pytest.raises(TypeError):
        extract_newest_time(test_var)


def test_function_ignores_files_without_leading_names():
    test_files = ["1234.csv", "hello5000.csv", "folder/32.csv"]
    result = extract_newest_time(test_files)
    assert result == 32


def test_function_ignores_non_csv_files():
    test_files = ["test_data.json", "dummy_file.txt", "file/1234.jpeg"]
    assert extract_newest_time(test_files) == 0
