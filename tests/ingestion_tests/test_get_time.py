from src.ingestion.ingestion_utils.get_time import get_time
from freezegun import freeze_time
from datetime import datetime


@freeze_time("2020-01-01 00:05:00")
def test_should_return_the_current_time():
    assert get_time() == datetime(2020, 1, 1, 0, 5, 0)
