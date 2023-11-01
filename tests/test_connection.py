from src.connection import get_connection
from unittest import TestCase
from pg8000.exceptions import InterfaceError
import pytest
from dotenv import dotenv_values


class TestConnection(TestCase):

    def test_connection_to_database_is_successful_with_correct_configuration(self): # noqa
        config = dotenv_values(".env")
        con = get_connection(user='peterkonstantynov',
                             password='password',
                             host='localhost',
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
