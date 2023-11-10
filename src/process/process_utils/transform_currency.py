import pandas as pd
from forex_python.converter import CurrencyCodes


def transform_currency(csv_file):
    """This function reads an ingested file of currency data.
    It then creates a dictionary with keys of currency_id,
    currency_code and currency_name, using a forex function
    to find corresponding currency codes.
    This dictionary is then converted into a dataframe and returned.

    Args:
        csv_file: a filepath to a csv file containing
        data ingested from the original database.
    Returns:
        a data frame containing all of the information
        that will be added to the dim_currency
        table in the new data warehouse.
    Raises:
        ValueError: if the specified columns do not appear
        in the .csv file.
    """
    try:
        data = pd.read_csv(csv_file, usecols=['currency_id', 'currency_code'])
        data_list = data.values.tolist()
        c = CurrencyCodes()
        currency_dict = {
            'currency_id': [item[0] for item in data_list],
            'currency_code': [item[1] for item in data_list],
            'currency_name': [
                c.get_currency_name(item[1]) for item in data_list
            ]
        }
        df = pd.DataFrame.from_dict(currency_dict)
        return df
    except ValueError as v:
        raise v
