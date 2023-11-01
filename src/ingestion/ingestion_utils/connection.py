from dotenv import dotenv_values, load_dotenv
from pg8000.native import Connection

load_dotenv()

if __name__ == "__main__":
    config = dotenv_values(".env")
    user = config["USER"]
    password = config["PASSWORD"]
    host = config["HOST"]
    port = config["PORT"]
    database = config["DATABASE"]


def get_connection(
    user=user, password=password, host=host, port=port, database=database
):
    """
    This function connects to the PSQL totesys database using pg8000.
    """
    con = Connection(
        user=user, password=password, host=host, port=port, database=database
    )
    return con
