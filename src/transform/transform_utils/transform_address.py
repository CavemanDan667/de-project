import awswrangler as wr
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
        ValueError: if the columns in the csv file are
        not as expected.
    """
    try:
        data = wr.s3.read_csv(
            path=csv_file,
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

    except ValueError as v:
        logger.error(f"transform_address has raised an error: {v}")
        raise v
    return data
