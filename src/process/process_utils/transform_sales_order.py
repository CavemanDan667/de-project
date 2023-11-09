import pandas as pd
from pg8000.native import DatabaseError, literal


def transform_sales_order(csv_file, conn):
    """
    This function reads an ingested file of sales data. For each sale,
    it checks to see if that combination of sales_id, updated_time and updated_date are within the fact_sales_order table. If it isn't, the function inserts the new sales data into the table, whether it is a new sale, or an update to a previous sale.

    Args:
        csv_file: a filepath to a csv file containing
        data ingested from the original database.
        conn: a connection to the new data warehouse.

     Returns:
        a data frame containing the section of the fact_sales_order table in the new data warehouse created by the function.

    Raises:
        DatabaseError: if either the select or insert
        query fails to match up to the destination
        table.
    """
    sales_order_data = pd.read_csv(csv_file)

    sales_order_list = sales_order_data.values.tolist()
    fact_sales_order_list = []

    for sale in sales_order_list:

        try:
            fact_sales_order_list.append([sale[0],
                                          sale[1][0:10],
                                          sale[1][11:19],
                                          sale[2][0:10],
                                          sale[2][11:19],
                                          sale[4],
                                          sale[5],
                                          sale[6],
                                          sale[7],
                                          sale[8],
                                          sale[3],
                                          sale[9],
                                          sale[10],
                                          sale[11]])

            select_query = f'''
                    SELECT * FROM fact_sales_order
                    WHERE sales_order_id = {literal(sale[0])}
                    AND last_updated_date = {literal(sale[2][0:10])}
                    AND last_updated_time = {literal(sale[2][11:19])};'''
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
                conn.run(insert_query)
        except DatabaseError as d:
            raise d
    fact_sales_order_df = pd.DataFrame(fact_sales_order_list)
    return fact_sales_order_df
