import awswrangler as wr
from pg8000.native import DatabaseError, literal
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_payment(parquet_file, conn):
    """
    This function reads parquet_file of transformed data.
    For each sale, it checks to see if that combination of payment_id,
    updated_time and updated_date are within the fact_payment table.
    If it isn't, the function inserts the new payment data into the table,
    whether it is a new payment, or an update to a previous payment.

    Args:
        parquet_file: a filepath to a parquet file containing
        data transformed from the original database.
        conn: a connection to the new data warehouse.

     Returns:
        a message confirming successful addition.

    Raises:
        DatabaseError: if either the select or insert
        query fails to match up to the destination
        table.
        KeyError/IndexError: if the columns in the passed parquet file
        do not match the expected columns.
    """
    payment_data = wr.s3.read_parquet(path=parquet_file, columns=[
        'payment_id', 'created_date', 'created_time',
        'last_updated_date', 'last_updated_time', 'transaction_id',
        'counterparty_id', 'payment_amount', 'currency_id',
        'payment_type_id', 'paid', 'payment_date'])

    payment_list = payment_data.values.tolist()
    print(payment_list)

    if len(payment_list) == 0:
        logger.error("load_payment was given an incorrect file")
        raise KeyError
    for payment in payment_list:
        try:
            select_query = f'''
                    SELECT * FROM fact_payment
                    WHERE payment_id = {literal(payment[0])}
                    AND last_updated_date = {literal(payment[3])}
                    AND last_updated_time = {literal(payment[4])};'''
            query_result = conn.run(select_query)
            if len(query_result) == 0:
                insert_query = f'''
                                INSERT INTO fact_payment
                                (payment_id,
                                created_date,
                                created_time,
                                last_updated_date,
                                last_updated_time,
                                transaction_id,
                                counterparty_id,
                                payment_amount,
                                currency_id,
                                payment_type_id,
                                paid,
                                payment_date)
                                VALUES (
                                    {literal(payment[0])},
                                    {literal(payment[1])},
                                    {literal(payment[2])},
                                    {literal(payment[3])},
                                    {literal(payment[4])},
                                    {literal(payment[5])},
                                    {literal(payment[6])},
                                    {literal(payment[7])},
                                    {literal(payment[8])},
                                    {literal(payment[9])},
                                    {literal(payment[10])},
                                    {literal(payment[11])}
                                );'''
            elif len(query_result) > 0:
                insert_query = f"""UPDATE fact_payment
                                SET created_date = {literal(payment[1])},
                                created_time = {literal(payment[2])},
                                last_updated_date = {literal(payment[3])},
                                last_updated_time = {literal(payment[4])},
                                transaction_id = {literal(payment[5])},
                                counterparty_id = {literal(payment[6])},
                                payment_amount = {literal(payment[7])},
                                currency_id = {literal(payment[8])},
                                payment_type_id = {literal(payment[9])},
                                paid = {literal(payment[10])},
                                payment_date = {literal(payment[11])}
                                WHERE payment_id = {literal(payment[0])}"""
            conn.run(insert_query)
        except DatabaseError as d:
            logger.error(f"load_payment has raised an error: {d}")
            raise d
        except IndexError as x:
            logger.error(f"load_payment has raised an error: {x}")
            raise x
    return 'Data loaded successfully - fact_payment'
