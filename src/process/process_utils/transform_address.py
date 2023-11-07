import pandas as pd
from pg8000.native import DatabaseError, literal


def transform_address(csv_file, conn):
    data = pd.read_csv(csv_file,
                       usecols=[
                           'address_id',
                           'address_line_1',
                           'address_line_2',
                           'district',
                           'city',
                           'postal_code',
                           'country',
                           'phone'
                       ])
    data_list = data.values.tolist()
    for item in data_list:
        try:
            select_query = f'''
                SELECT * FROM dim_location
                WHERE location_id = {literal(item[0])}'''
            select_result = conn.run(select_query)
            if len(select_result) == 0:
                if type(item[2]) is not str:
                    item[2] = None
                if type(item[3]) is not str:
                    item[3] = None
                query = f'''INSERT INTO dim_location (
            location_id, address_line_1,
            address_line_2, district,
            city, postal_code,
            country, phone
            ) VALUES (
            {literal(item[0])}, {literal(item[1])},
            {literal(item[2])}, {literal(item[3])},
            {literal(item[4])}, {literal(item[5])},
            {literal(item[6])}, {literal(item[7])}
            )'''
                conn.run(query)
        except DatabaseError as d:
            raise d
    return data
