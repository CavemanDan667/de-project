from src.process.process_utils.extract_filepath import extract_filepath
from test_event import s3_put


def test_returns_S3_URI_in_correct_form():
    assert extract_filepath(s3_put) == "s3://mybucket/test-folder/12345.csv"
