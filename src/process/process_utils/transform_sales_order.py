import pandas as pd
from pg8000.native import DatabaseError, literal


def transform_sales_order(csv_file, conn):
    """
    This function reads an ingested file of sales data. For each sale,
    it checks to see if the sales_id already exists within the
    fact_sales_order table. If it does, it updates the table with the
    correct information. If it doesn't, it inserts the new sales
    data.

    Args:
        csv_file: a filepath to a csv file containing
        data ingested from the original database.
        conn: a connection to the new data warehouse.

     Returns:
        a data frame containing all of the information
        within the fact_sales_order table in the new data warehouse.

    Raises:
        DatabaseError: if either the select or insert
        query fails to match up to the destination
        table.
    """
    sales_order_data = pd.read_csv(csv_file,
                                   usecols=['sales_order_id',
                                            'created_at',
                                            'last_updated',
                                            'design_id',
                                            'staff_id',
                                            'counterparty_id',
                                            'units_sold',
                                            'unit_price',
                                            'currency_id',
                                            'agreed_delivery_date',
                                            'agreed_payment_date',
                                            'agreed_delivery_location_id'])

    sales_order_list = sales_order_data.values.tolist()

    for sale in sales_order_list:
        try:
            select_query = f'''
                    SELECT * FROM fact_sales_order
                    WHERE sales_order_id = {literal(sale[0])};'''
            query_result = conn.run(select_query)
            if len(query_result) == 0:
                insert_query = f'''
                                INSERT INTO fact_sales_order
                                (sales_order_id,
                                created_date,
                                created_time,
                                last_updated_date,
                                last_updated_time,
                                sales_staff_id,
                                counterparty_id,
                                units_sold,
                                unit_price,
                                currency_id,
                                design_id,
                                agreed_payment_date,
                                agreed_delivery_date,
                                agreed_delivery_location_id)
                                VALUES (
                                    {literal(sale[0])},
                                    {literal(sale[1][0:10])},
                                    {literal(sale[1][11:19])},
                                    {literal(sale[2][0:10])},
                                    {literal(sale[2][11:19])},
                                    {literal(sale[4])},
                                    {literal(sale[5])},
                                    {literal(sale[6])},
                                    {literal(sale[7])},
                                    {literal(sale[8])},
                                    {literal(sale[3])},
                                    {literal(sale[9])},
                                    {literal(sale[10])},
                                    {literal(sale[11])}
                                );'''
            elif len(query_result) > 0:
                insert_query = f'''
                    UPDATE fact_sales_order
                    SET created_date = {literal(sale[1][0:10])},
                    created_time = {literal(sale[1][11:19])},
                    last_updated_date = {literal(sale[2][0:10])},
                    last_updated_time = {literal(sale[2][11:19])},
                    sales_staff_id = {literal(sale[4])},
                    counterparty_id = {literal(sale[5])},
                    units_sold = {literal(sale[6])},
                    unit_price = {literal(sale[7])},
                    currency_id = {literal(sale[8])},
                    design_id = {literal(sale[3])},
                    agreed_payment_date = {literal(sale[9])},
                    agreed_delivery_date = {literal(sale[10])},
                    agreed_delivery_location_id = {literal(sale[11])}
                    WHERE sales_order_id = {literal(sale[0])}
                    ;'''
            conn.run(insert_query)
        except DatabaseError as d:
            raise d

    fact_sales_order_data = pd.DataFrame(conn.run(
        'SELECT * FROM fact_sales_order'))

    return fact_sales_order_data
