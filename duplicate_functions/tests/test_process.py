# from src.process.process import handler
from unittest.mock import patch
import pytest


@pytest.fixture
def mock_extract_filepath():
    with patch(
        "src.process.process.extract_filepath",
        return_value="Test_filepath",
    ) as filepath_patch:
        yield filepath_patch


@pytest.fixture
def mock_extract_event_data():
    with patch(
        "src.process.process.extract_event_data",
        return_value=("currency", "Test_unix"),
    ) as extract_patch:
        yield extract_patch


@pytest.fixture
def mock_transform_currency():
    with patch(
        "src.process.process.transform_currency",
        return_value="Test_data_frame",
    ) as transform_currency_patch:
        yield transform_currency_patch


@pytest.fixture
def mock_connection():
    with patch(
        "src.process.process.Connection",
        return_value="Test_connection",
    ) as connection_patch:
        yield connection_patch


@pytest.fixture
def mock_write():
    with patch(
        "src.process.process.write_data_to_parquet",
        return_value="Parquet Created",
    ) as write_patch:
        yield write_patch


# def test_handler_called_with_event_object_with_valid_table_name(
#     mock_extract_event_data,
#     mock_extract_filepath,
#     mock_transform_currency,
#     mock_connection,
#     mock_write
# ):
#     handler("Test_event", "context")
#     assert mock_extract_event_data.call_args == call('Test_event')
#     assert mock_extract_filepath.call_args == call('Test_event')
#     assert mock_transform_currency.call_args == call(
#         'Test_connection',
#         'Test_filepath'
#     )
#     assert mock_write.call_args == call(
#         'Test_unix',
#         'currency',
#         'Test_data_frame'
#     )


# def test_handler_raises_and_logs_errors(caplog):
#     with pytest.raises(Exception):
#         handler("Test_event", "context")
#     assert 'Process handler has raised an error' in caplog.text