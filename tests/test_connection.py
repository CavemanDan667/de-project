from ingestion.ingestion_utils.connection import get_connection
from unittest import TestCase
from unittest.mock import MagicMock, patch
from pg8000.exceptions import InterfaceError
import pytest


class TestConnection(TestCase):
    @patch("src.connection.get_connection")
    def test_database(self, mock_connect):
        get_connection(
            user='peterkonstantynov',
            password='password',
            host='127.0.0.1',
            port=5432,
            database='test'
        )
        mock_connect.return_value = MagicMock()
        self.assertIsNotNone(mock_connect)

    def test_connection_to_database_is_successful_with_correct_configuration(self): # noqa
        con = get_connection(user='peterkonstantynov',
                             password='password',
                             host='127.0.0.1',
                             port=5432,
                             database='test')
        self.assertIsNotNone(con)
        con.close()

    def test_connection_raises_interface_error_when_incorrect_configuration_used(self): # noqa
        with pytest.raises(InterfaceError):
            get_connection(user="jason",
                           password="password123",
                           host="nc-database",
                           port=1234,
                           database="db")
