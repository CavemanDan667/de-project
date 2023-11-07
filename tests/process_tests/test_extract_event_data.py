# flake8: noqa
from src.process.process_utils.extract_event_data import extract_event_data
from test_event import s3_put, broken_s3_put
import pytest


def test_should_return_date_and_table_name_when_a_csv_is_put_into_the_bucket():
    assert extract_event_data(s3_put) == ("test-folder", "12345")


def test_should_assert_a_key_error_and_log_appropriately_if_event_is_missing_required_keys(
    caplog,
):
    with pytest.raises(KeyError):
        extract_event_data(broken_s3_put)
    assert "extract_event_data has raised" in caplog.text
