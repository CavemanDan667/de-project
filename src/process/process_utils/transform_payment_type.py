import pandas as pd


def transform_payment_type(csv_file):
    """This function reads an ingested file of payment type
    data. It takes the first two columns from that file,
    and builds a data frame that it then returns.

    Args:
        csv_file: a filepath to a csv file containing
        data ingested from the original database.
    Returns:
        a data frame containing all of the information
        that will be added to the dim_payment_type
        table in the new data warehouse.
    Raises:
        KeyError: if the columns in the csv file are
        not as expected.
    """
    try:
        data = pd.read_csv(csv_file, usecols=[
                            'payment_type_id',
                            'payment_type_name'
                            ])
        return data
    except KeyError as k:
        raise k
