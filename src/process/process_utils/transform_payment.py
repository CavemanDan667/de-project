import pandas as pd
import awswrangler as wr
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def transform_payment(csv_file):
    """
    This function reads an ingested file of payment data.
    It transforms that data into a re-ordered list, splitting
    the datetimes into separate date and time entries.
    It then converts that list into a dataframe and returns.

    Args:
        csv_file: a filepath to a csv file containing
        data ingested from the original database.

     Returns:
        a data frame containing the section of the fact_payment table
        to be added to the new data warehouse created by the function.

    Raises:
        ValueError: if the passed .csv file does not contain
        the expected columns.
        IndexError: if the datetime input does not match the expected length.
    """

    payment_data = wr.s3.read_csv(path=csv_file, usecols=[
        'payment_id', 'created_at', 'last_updated',
        'transaction_id', 'counterparty_id', 'payment_amount',
        'currency_id', 'payment_type_id', 'paid',
        'payment_date', 'company_ac_number', 'counterparty_ac_number'
    ])

    payment_list = payment_data.values.tolist()
    fact_payment_list = []
    try:
        for payment in payment_list:
            fact_payment_list.append([payment[0],
                                      payment[1][0:10],
                                      payment[1][11:19],
                                      payment[2][0:10],
                                      payment[2][11:19],
                                      payment[3],
                                      payment[4],
                                      payment[5],
                                      payment[6],
                                      payment[7],
                                      payment[8],
                                      payment[9]])
    except ValueError as v:
        logger.error(f"transform_payment has raised an error: {v}")
        raise v
    except IndexError as x:
        logger.error(f"transform_payment has raised an error: {x}")
        raise x
    fact_payment_df = pd.DataFrame(fact_payment_list, columns=[
        'payment_id', 'created_date', 'created_time',
        'last_updated_date', 'last_updated_time', 'transaction_id',
        'counterparty_id', 'payment_amount', 'currency_id',
        'payment_type_id', 'paid', 'payment_date'])
    return fact_payment_df
