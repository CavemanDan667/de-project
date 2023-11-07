from src.process.process import handler
from unittest.mock import Mock, MagicMock, patch
import pytest

@pytest.fixture
def mock_extract_event_data():
    m_extract = MagicMock()
    with patch('extract_event_data', return_value=m_extract) as extract_patch:
        yield extract_patch


def test_extract_event_data_called_with_event_object(mock_extract_event_data):
    handler({"Key": "Event"})
    print(dir(mock_extract_event_data))
    assert False

