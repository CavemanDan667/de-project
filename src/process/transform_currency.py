import pandas as pd
from forex_python.converter import CurrencyCodes
from pg8000.native import DatabaseError, literal


def transform_currency(csv_file, conn):
    data = pd.read_csv(csv_file, usecols=['currency_code'])
    data_list = data.values.tolist()
    c = CurrencyCodes()
    currency_dict = {
        'currency_code': [item[0] for item in data_list],
        'currency_name': [c.get_currency_name(item[0]) for item in data_list]
    }
    df = pd.DataFrame.from_dict(currency_dict)
    for value in df.values.tolist():
        try:
            query = f'INSERT INTO dim_currency (currency_code, currency_name) VALUES ({literal(value[0])}, {literal(value[1])});'
            conn.run(query)
        except DatabaseError as d:
            raise d
    return df
