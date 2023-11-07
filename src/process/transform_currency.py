import pandas as pd
from forex_python.converter import CurrencyCodes
from pg8000.native import DatabaseError, literal


def transform_currency(csv_file, conn):
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
            print(select_query)
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
