from src.process.process_utils.extract_filepath import extract_filepath
from test_event import s3_put
import pytest


def test_returns_S3_URI_in_correct_form():
    assert extract_filepath(s3_put) == "s3://mybucket/test-folder/12345.csv"


def test_raises_KeyError_if_key_missing(caplog):
    s3_put = {}
    with pytest.raises(KeyError):
        extract_filepath(s3_put)
    msg = "extract_filepath has raised KeyError: key not found: "
    assert msg in caplog.text
