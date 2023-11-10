import awswrangler as wr
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def transform_design(csv_file):
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
        data = wr.s3.read_csv(
            csv_file, usecols=["design_id", "design_name",
                               "file_location", "file_name"]
        )
        return data
    except KeyError as k:
        raise k
    except ValueError as v:
        logger.error(f"Load handler has raised an error: {v}")
        raise v
