import pandas as pd
from pg8000.native import literal, DatabaseError


def transform_counterparty(csv_file, conn):
    address_data = conn.run('SELECT * FROM dim_location;')
    address_dict = {item[0]: item[1:] for item in address_data}
    counterparty_data = pd.read_csv(csv_file,
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
    for row in counterparty_frame.values.tolist():
        try:
            select_query = f'''SELECT * FROM dim_counterparty
            WHERE counterparty_id = {literal(row[0])};'''
            query_result = conn.run(select_query)
            if len(query_result) == 0:
                insert_query = f'''INSERT INTO dim_counterparty (
                    counterparty_id,
                    counterparty_legal_name,
                    counterparty_legal_address_line_1,
                    counterparty_legal_address_line_2,
                    counterparty_legal_district,
                    counterparty_legal_city,
                    counterparty_legal_postal_code,
                    counterparty_legal_country,
                    counterparty_legal_phone_number
                    ) VALUES (
                    {literal(row[0])},
                    {literal(row[1])},
                    {literal(row[2])},
                    {literal(row[3])},
                    {literal(row[4])},
                    {literal(row[5])},
                    {literal(row[6])},
                    {literal(row[7])},
                    {literal(row[8])});'''
            elif len(query_result) > 0:
                insert_query = f'''UPDATE dim_counterparty
                    SET counterparty_legal_name = {literal(row[1])},
                    counterparty_legal_address_line_1 = {literal(row[2])},
                    counterparty_legal_address_line_2 = {literal(row[3])},
                    counterparty_legal_district = {literal(row[4])},
                    counterparty_legal_city = {literal(row[5])},
                    counterparty_legal_postal_code = {literal(row[6])},
                    counterparty_legal_country = {literal(row[7])},
                    counterparty_legal_phone_number = {literal(row[8])}
                    WHERE counterparty_id = {literal(row[0])}'''
                conn.run(insert_query)
        except DatabaseError as d:
            raise d
    return counterparty_frame
