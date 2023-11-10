import pandas as pd
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def transform_address(csv_file):
    """This function reads an ingested file of design data.
    It takes four columns from that file and builds a data
    frame that it then returns.

    Args:
        csv_file: a filepath to a csv file containing
        data ingested from the original database.
    Returns:
        a data frame containing all of the information
        that has been added to the dim_design
        table in the new data warehouse.
    Raises:
        KeyError: if the columns in the csv file are
        not as expected.
    """
    try:
        data = pd.read_csv(
            csv_file,
            usecols=[
                "address_id",
                "address_line_1",
                "address_line_2",
                "district",
                "city",
                "postal_code",
                "country",
                "phone",
            ],
        )
        return data
    except KeyError as k:
        raise k
    except ValueError as v:
        logger.error(f"Load handler has raised an error: {v}")
        raise v
