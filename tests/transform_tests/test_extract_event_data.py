from src.transform.transform_utils.extract_event_data import extract_event_data
from test_event import s3_put, broken_s3_put
import pytest


def test_returns_date_and_table_name_when_a_csv_is_put_into_the_bucket():
    assert extract_event_data(s3_put) == ("test-folder", "12345")


def test_asserts_error_and_logs_appropriately_if_event_missing_required_keys(
    caplog,
):
    with pytest.raises(KeyError):
        extract_event_data(broken_s3_put)
    assert "extract_event_data has raised" in caplog.text
