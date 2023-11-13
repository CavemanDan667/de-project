import pandas as pd
import awswrangler as wr
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def transform_counterparty(csv_file, conn):
    """This function reads an ingested file from a
    counterparty table, queries the dim_location table
    in the new data warehouse to populate a dictionary
    of addresses, and then uses this dictionary to
    replace address ids in the original file with
    full corresponding address details in the new data.
    Returns a DataFrame.

    Args:
        csv_file: a filepath to a csv file containing
        data ingested from the original database.
        conn: a connection to the new data warehouse.
    Returns:
        a data frame containing all of the information
        that has been added to the dim_counterparty
        table in the new data warehouse.
    Raises:
        KeyError: if the columns in the csv file are
        not as expected.
    """
    try:
        address_data = conn.run('SELECT * FROM dim_location;')
        address_dict = {item[0]: item[1:] for item in address_data}
        counterparty_data = wr.s3.read_csv(path=csv_file,
                                           usecols=[
                                            'counterparty_id',
                                            'counterparty_legal_name',
                                            'legal_address_id'
                                            ])
        counterparty_list = counterparty_data.values.tolist()
        counterparty_dict = {
            'counterparty_id': [item[0] for item in counterparty_list],
            'counterparty_legal_name': [item[1] for item in counterparty_list],
            'counterparty_legal_address_line_1': [
                address_dict[item[2]][0] for item in counterparty_list
                ],
            'counterparty_legal_address_line_2': [
                address_dict[item[2]][1] for item in counterparty_list
                ],
            'counterparty_legal_district': [
                address_dict[item[2]][2] for item in counterparty_list
                ],
            'counterparty_legal_city': [
                address_dict[item[2]][3] for item in counterparty_list
                ],
            'counterparty_legal_postal_code': [
                address_dict[item[2]][4] for item in counterparty_list
                ],
            'counterparty_legal_country': [
                address_dict[item[2]][5] for item in counterparty_list
                ],
            'counterparty_legal_phone_number': [
                address_dict[item[2]][6] for item in counterparty_list
                ]
        }
        counterparty_frame = pd.DataFrame.from_dict(counterparty_dict)
    except KeyError as k:
        raise k
    except ValueError as v:
        logger.error(f'Load handler has raised an error: {v}')
        raise v
    return counterparty_frame
