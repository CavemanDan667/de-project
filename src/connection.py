from dotenv import dotenv_values
from pg8000.native import Connection


config = dotenv_values(".env")


def get_connection():
    """
    This function connects to the PSQL totesys database using pg8000.
    """
    return Connection(
        user=config["USER"],
        password=config["PASSWORD"],
        host=config["HOST"],
        port=config["PORT"],
        database=config["DATABASE"],
    )
