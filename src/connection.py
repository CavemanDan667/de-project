from dotenv import dotenv_values
from pg8000.native import Connection


config = dotenv_values(".env")

user = config["USER"]
password = config["PASSWORD"]
host = config["HOST"]
port = config["PORT"]
database = config["DATABASE"]


def get_connection(user, password, host, port, database):
    """
    This function connects to the PSQL totesys database using pg8000.
    """
    con = Connection(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database
    )
    return con
