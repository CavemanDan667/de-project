# flake8: noqa
from src.process.process_utils.extract_event_data import extract_event_data
from test_event import s3_put



def test_should_return_date_and_table_name_when_a_csv_is_put_into_the_bucket():
    assert extract_event_data(s3_put) == ('test-folder', '12345')