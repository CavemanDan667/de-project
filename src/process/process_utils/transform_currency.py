import pandas as pd
from forex_python.converter import CurrencyCodes
from pg8000.native import DatabaseError, literal


def transform_currency(csv_file, conn):
    """This function reads an ingested file of currency data.
    It then creates a dictionary with keys of currency_id,
    currency_code and currency_name. This dictionary is then converted
    into a DataFrame. This function then checks whether each currency_id
    in the DataFrame appears in the dim_currency table. If the currency_id is
    not found in dim_currency, this function adds the relevant
    data to dim_currency.

    Args:
        csv_file: a filepath to a csv file containing
        data ingested from the original database.
        conn: a connection to the new data warehouse.
    Returns:
        a data frame containing all of the information
        that has been added to the dim_currency
        table in the new data warehouse.
    Raises:
        DatabaseError: if either the select or insert
        query fails to match up to the destination
        table.
    """
    data = pd.read_csv(csv_file, usecols=['currency_id', 'currency_code'])
    data_list = data.values.tolist()
    c = CurrencyCodes()
    currency_dict = {
        'currency_id': [item[0] for item in data_list],
        'currency_code': [item[1] for item in data_list],
        'currency_name': [c.get_currency_name(item[1]) for item in data_list]
    }
    df = pd.DataFrame.from_dict(currency_dict)
    for value in df.values.tolist():
        try:
            select_query = f'''SELECT * FROM dim_currency
            WHERE currency_code = {literal(value[1])};'''
            result = conn.run(select_query)
            if len(result) == 0:
                insert_query = f'''INSERT INTO dim_currency
                (currency_id, currency_code, currency_name)

                VALUES (
                    {literal(value[0])},
                    {literal(value[1])},
                    {literal(value[2])}
                );'''

                conn.run(insert_query)
        except DatabaseError as d:
            raise d
    return df
