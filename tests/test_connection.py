from src.connection import get_connection
from unittest import TestCase
from pg8000.exceptions import InterfaceError
import pytest
from dotenv import dotenv_values


class TestConnection(TestCase):

    def test_connection_to_database_is_successful_with_correct_configuration(self): # noqa
        config = dotenv_values(".env")
        user = config["USER"]
        password = config["PASSWORD"]
        host = config["HOST"]
        port = config["PORT"]
        database = config["DATABASE"]
        con = get_connection(user=user,
                             password=password,
                             host=host,
                             port=port,
                             database=database)
        self.assertIsNotNone(con)
        con.close()

    def test_connection_raises_interface_error_when_incorrect_configuration_used(self): # noqa
        user = "jason"
        password = "password123"
        host = "nc-database"
        port = 1234
        database = "db"
        with pytest.raises(InterfaceError):
            get_connection(user=user,
                           password=password,
                           host=host,
                           port=port,
                           database=database)
